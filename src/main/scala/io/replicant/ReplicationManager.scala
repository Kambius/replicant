package io.replicant

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import cats.syntax.partialOrder._
import io.replicant.common.instances.vectorclock._

object ReplicationManager {
  final val Name: String = "replication-manager"

  sealed trait ReplicationCommand

  final case class Get(
      key: String,
      replyTo: ActorRef[GetRequestActor.ReplicaResponse]
  ) extends ReplicationCommand

  final case class Put(
      key: String,
      value: Option[String],
      clock: Map[String, Int],
      replyTo: ActorRef[PutRequestActor.ReplicaResponse]
  ) extends ReplicationCommand

  def behavior(storage: Storage): Behavior[ReplicationCommand] =
    Behaviors.receiveMessage {
      case Get(key, replyTo) =>
        replyTo ! GetRequestActor.ReplicaResponse(storage.get(key))
        Behaviors.same

      case Put(key, value, clock, replyTo) =>
        val old = storage.get(key)
        if (old.clock.tryCompare(clock).exists(_ <= 0)) {
          storage.put(key, key.##, value, clock)
          replyTo ! PutRequestActor.ReplicaResponse(true)
        } else {
          replyTo ! PutRequestActor.ReplicaResponse(false)
        }
        Behaviors.same
    }
}
