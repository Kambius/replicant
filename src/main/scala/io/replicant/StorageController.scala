package io.replicant

import akka.actor.Scheduler
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import cats.syntax.either._
import cats.syntax.option._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.semiauto._
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor}
import io.replicant.StorageController._
import io.replicant.StorageManager.StorageCommand

import scala.concurrent.duration._
import scala.util.Success

class StorageController(storageManager: ActorRef[StorageCommand], system: ActorSystem[_]) {
  private implicit val timeout: Timeout     = 3.seconds
  private implicit val scheduler: Scheduler = system.scheduler

  val route: Route =
    path("api") {
      post {
        entity(as[StorageRequest]) { request =>
          val response = request match {
            case GetReq(key, replicas) =>
              storageManager ? { ref: ActorRef[StorageDataResult] =>
                StorageManager.Get(key, replicas, ref)
              }

            case PutReq(key, value, replicas) =>
              storageManager ? { ref: ActorRef[StorageModificationResult] =>
                StorageManager.Put(key, value.some, Map.empty, replicas, ref)
              }

            case ModReq(key, value, clock, replicas) =>
              storageManager ? { ref: ActorRef[StorageModificationResult] =>
                StorageManager.Put(key, value.some, clock, replicas, ref)
              }

            case DelReq(key, clock, replicas) =>
              storageManager ? { ref: ActorRef[StorageModificationResult] =>
                StorageManager.Put(key, none, clock, replicas, ref)
              }
          }

          onComplete(response) {
            case Success(StorageResult.Success)             => complete(StatusCodes.OK)
            case Success(StorageResult.Data(values, clock)) => complete(SuccessResp(values, clock))
            case Success(StorageResult.Failure(err))        => complete(StatusCodes.BadRequest -> FailureResp(err))
            case _                                          => complete(StatusCodes.InternalServerError)
          }
        }
      }
    }
}

object StorageController {
  sealed trait StorageRequest
  final case class GetReq(key: String, replicas: Int)                                         extends StorageRequest
  final case class PutReq(key: String, value: String, replicas: Int)                          extends StorageRequest
  final case class ModReq(key: String, value: String, clock: Map[String, Int], replicas: Int) extends StorageRequest
  final case class DelReq(key: String, clock: Map[String, Int], replicas: Int)                extends StorageRequest

  object StorageRequest {
    private implicit val getDecoder: Decoder[GetReq] = deriveDecoder
    private implicit val putDecoder: Decoder[PutReq] = deriveDecoder
    private implicit val modDecoder: Decoder[ModReq] = deriveDecoder
    private implicit val delDecoder: Decoder[DelReq] = deriveDecoder

    implicit val decoder: Decoder[StorageRequest] = (c: HCursor) =>
      c.downField("type").as[String] match {
        case Right("get") => c.as[GetReq]
        case Right("put") => c.as[PutReq]
        case Right("mod") => c.as[ModReq]
        case Right("del") => c.as[DelReq]
        case Right(s)     => DecodingFailure(s"Unknown operation type: $s", c.history).asLeft
        case Left(_)      => DecodingFailure("Type of operation is not specified", c.history).asLeft
    }
  }

  sealed trait StorageResponse
  final case class SuccessResp(values: Set[String], clock: Map[String, Int]) extends StorageResponse
  final case class FailureResp(message: String)                              extends StorageResponse

  object StorageResponse {
    implicit val successEncoder: Encoder[SuccessResp] = deriveEncoder
    implicit val failureEncoder: Encoder[FailureResp] = deriveEncoder
  }
}
