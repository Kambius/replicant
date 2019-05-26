package io.replicant

import akka.actor.RootActorPath
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.MemberStatus
import akka.cluster.typed.Cluster
import cats.syntax.partialOrder._
import io.replicant.common.instances.vectorclock._

import scala.concurrent.duration._

object PutRequestActor {
  sealed trait PutFlowCommand
  case object Timeout                                   extends PutFlowCommand
  final case class ReplicaResponse(successful: Boolean) extends PutFlowCommand

  private final val TimeoutTimer: String        = "timeout"
  private final val ReadTimeout: FiniteDuration = 1.second

  private def incrementClock(clock: Map[String, Int], node: String): Map[String, Int] =
    clock + (node -> (clock.getOrElse(node, 0) + 1))

  private def getNodeId(cluster: Cluster): String =
    cluster.selfMember.address.host.getOrElse("") + ":" + cluster.selfMember.address.port.getOrElse("")

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

  def behavior(storage: Storage, req: StorageManager.Put): Behavior[PutFlowCommand] =
    Behaviors.withTimers { scheduler =>
      scheduler.startSingleTimer(TimeoutTimer, Timeout, ReadTimeout)

      Behaviors.setup { ctx =>
        val cluster = Cluster(ctx.system)

        val old = storage.get(req.key)

        if (old.clock.tryCompare(req.clock).exists(_ <= 0)) {
          val newClock = incrementClock(req.clock, getNodeId(cluster))

          cluster.state.members.filter(m => m.status == MemberStatus.Up && m != cluster.selfMember).foreach { m =>
            ctx.toUntyped
              .actorSelection(RootActorPath(m.address) / "user" / ReplicationManager.Name) ! ReplicationManager
              .Put(req.key, req.value, newClock, ctx.self)
          }

          if (req.replicas > 1) {
            waitResponses(req, 1)
          } else {
            req.replyTo ! StorageResult.Success
            Behaviors.stopped
          }
        } else {
          req.replyTo ! StorageResult.Failure("Outdated data")
          Behaviors.stopped
        }
      }
    }
}
