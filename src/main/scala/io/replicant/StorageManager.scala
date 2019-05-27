package io.replicant

import akka.actor.typed._
import akka.actor.typed.scaladsl._

object StorageManager {
  sealed trait StorageCommand {
    def replicas: Int
  }

  final case class Get(
      key: String,
      replicas: Int,
      replyTo: ActorRef[StorageDataResult]
  ) extends StorageCommand

  final case class Put(
      key: String,
      value: Option[String],
      clock: Map[String, Int],
      replicas: Int,
      replyTo: ActorRef[StorageModificationResult]
  ) extends StorageCommand

  val behavior: Behavior[StorageCommand] =
    Behaviors.receive {
      case (ctx, get: Get) =>
        ctx.spawnAnonymous(GetRequestActor.behavior(get))
        Behaviors.same

      case (ctx, put: Put) =>
        ctx.spawnAnonymous(PutRequestActor.behavior(put))
        Behaviors.same
    }
}

sealed trait StorageResult
sealed trait StorageDataResult         extends StorageResult
sealed trait StorageModificationResult extends StorageDataResult
object StorageResult {
  case object Success                                                 extends StorageModificationResult
  final case class Data(values: Set[String], clock: Map[String, Int]) extends StorageDataResult
  final case class Failure(error: String)                             extends StorageDataResult with StorageModificationResult
}
