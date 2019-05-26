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

object GetRequestActor {
  sealed trait GetFlowCommand
  case object Timeout                                    extends GetFlowCommand
  final case class ReplicaResponse(value: Storage.Value) extends GetFlowCommand

  private final val TimeoutTimer: String        = "timeout"
  private final val ReadTimeout: FiniteDuration = 1.second

  private def mergeClocks(x: Map[String, Int], y: Map[String, Int]): Map[String, Int] =
    x.keySet.union(y.keySet).map(k => k -> x.getOrElse(k, 0).max(y.getOrElse(k, 0))).toMap

  private def resolveValues(values: Seq[Storage.Value]): StorageResult.Data = {
    val merged = values.foldLeft(Seq.empty[(Option[String], Map[String, Int])]) { (acc, v) =>
      val idx = acc.indexWhere(_._2.tryCompare(v.clock).isDefined)

      if (idx == -1) {
        acc :+ (v.data -> v.clock)
      } else {
        val (_, clock) = acc(idx)
        val ord        = v.clock.partialCompare(clock)
        if (ord > 0) {
          acc.patch(idx, Seq(v.data -> v.clock), 1)
        } else {
          acc
        }
      }
    }

    StorageResult.Data(merged.flatMap(_._1).toSet, merged.map(_._2).fold(Map.empty)(mergeClocks))
  }

  private def waitResponses(req: StorageManager.Get, collectedValues: Seq[Storage.Value]): Behavior[GetFlowCommand] =
    Behaviors.receive {
      case (_, ReplicaResponse(value)) =>
        val nextCollected = value +: collectedValues
        if (nextCollected.size == req.replicas) {
          req.replyTo ! resolveValues(nextCollected)
          Behaviors.stopped
        } else {
          waitResponses(req, nextCollected)
        }

      case (ctx, Timeout) =>
        ctx.log.warning("Request timeout. Collected {} of {} values", collectedValues.size, req.replicas)
        req.replyTo ! StorageResult.Failure("Request timeout")
        Behaviors.stopped
    }

  def behavior(storage: Storage, req: StorageManager.Get): Behavior[GetFlowCommand] =
    Behaviors.withTimers { scheduler =>
      scheduler.startSingleTimer(TimeoutTimer, Timeout, ReadTimeout)

      Behaviors.setup { ctx =>
        val cluster = Cluster(ctx.system)

        if (req.replicas > 1) {
          cluster.state.members.filter(m => m.status == MemberStatus.Up && m != cluster.selfMember).foreach { m =>
            ctx.toUntyped
              .actorSelection(RootActorPath(m.address) / "user" / ReplicationManager.Name) ! ReplicationManager
              .Get(req.key, ctx.self)
          }

          waitResponses(req, Seq(storage.get(req.key)))
        } else {
          req.replyTo ! resolveValues(Seq(storage.get(req.key)))
          Behaviors.stopped
        }
      }
    }
}
