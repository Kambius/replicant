package io.replicant

import akka.actor.RootActorPath
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.Member
import akka.cluster.typed.Cluster

import scala.concurrent.duration._

object PutRequestActor {
  sealed trait PutFlowCommand
  case object Timeout                                   extends PutFlowCommand
  final case class ReplicaResponse(successful: Boolean) extends PutFlowCommand

  private final val TimeoutTimer: String         = "timeout"
  private final val WriteTimeout: FiniteDuration = 1.second

  private def incrementClock(clock: Map[String, Int], node: String): Map[String, Int] =
    clock + (node -> (clock.getOrElse(node, 0) + 1))

  private def getNodeId(member: Member): String =
    member.address.host.getOrElse("") + ":" + member.address.port.getOrElse("")

  private def waitResponses(req: StorageManager.Put, collectedResponses: Int): Behavior[PutFlowCommand] =
    Behaviors.receive {
      case (_, ReplicaResponse(successful)) =>
        if (successful) {
          val nextCollected = collectedResponses + 1
          if (nextCollected == req.replicas) {
            req.replyTo ! StorageResult.Success
            Behaviors.stopped
          } else {
            waitResponses(req, nextCollected)
          }
        } else {
          req.replyTo ! StorageResult.Failure("Outdated data")
          Behaviors.stopped
        }

      case (ctx, Timeout) =>
        ctx.log.warning("Request timeout. Collected {} of {} responses", collectedResponses, req.replicas)
        req.replyTo ! StorageResult.Failure("Request timeout")
        Behaviors.stopped
    }

  def behavior(req: StorageManager.Put): Behavior[PutFlowCommand] =
    Behaviors.withTimers { scheduler =>
      scheduler.startSingleTimer(TimeoutTimer, Timeout, WriteTimeout)

      Behaviors.setup { ctx =>
        val replicationLevel = ctx.system.settings.config.getInt("replicant.replication-level")

        if (req.replicas <= 0 || req.replicas > replicationLevel) {
          req.replyTo ! StorageResult.Failure("Invalid replicas count")
          Behaviors.stopped
        } else {
          val cluster    = Cluster(ctx.system)
          val replicaSet = ConsistentHashing.getReplicaSet(cluster.state.members, replicationLevel, req.key)

          val newClock = incrementClock(req.clock, getNodeId(replicaSet.head))

          replicaSet.foreach { m =>
            ctx.toUntyped
              .actorSelection(RootActorPath(m.address) / "user" / ReplicationManager.Name) ! ReplicationManager
              .Put(req.key, req.value, newClock, ctx.self)
          }

          waitResponses(req, 0)
        }
      }
    }
}
