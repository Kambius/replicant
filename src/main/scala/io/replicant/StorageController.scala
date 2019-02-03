package io.replicant

import akka.actor.Scheduler
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import cats.syntax.either._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.semiauto._
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.replicant.StorageController._
import io.replicant.StorageManager.{StorageCommand, StorageDataResult, StorageModificationResult, StorageResult}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

class StorageController(storageManager: ActorRef[StorageCommand], system: ActorSystem[_]) {
  private implicit val timeout: Timeout     = 3.seconds
  private implicit val scheduler: Scheduler = system.scheduler

  val route: Route =
    path("api") {
      post {
        entity(as[StorageOperation]) {
          case Get(key) =>
            val response: Future[StorageDataResult] =
              storageManager ? (ref => StorageCommand.Get(key, ref))

            onComplete(response) {
              case Success(StorageResult.Data(value))  => complete(value)
              case Success(StorageResult.Failure(err)) => complete(StatusCodes.BadRequest -> err)
              case _                                   => complete(StatusCodes.InternalServerError)
            }

          case Put(key, value, consistency) =>
            val response: Future[StorageModificationResult] =
              storageManager ? (ref => StorageCommand.Put(key, value, ref, consistency))

            onComplete(response) {
              case Success(StorageResult.Success)      => complete(StatusCodes.OK)
              case Success(StorageResult.Failure(err)) => complete(StatusCodes.BadRequest -> err)
              case _                                   => complete(StatusCodes.InternalServerError)
            }

          case Del(key, consistency) =>
            val response: Future[StorageModificationResult] =
              storageManager ? (ref => StorageCommand.Del(key, ref, consistency))

            onComplete(response) {
              case Success(StorageResult.Success)      => complete(StatusCodes.OK)
              case Success(StorageResult.Failure(err)) => complete(StatusCodes.BadRequest -> err)
              case _                                   => complete(StatusCodes.InternalServerError)
            }
        }
      }
    }
}

object StorageController {
  sealed trait StorageOperation
  final case class Get(key: String)                                  extends StorageOperation
  final case class Put(key: String, value: String, consistency: Int) extends StorageOperation
  final case class Del(key: String, consistency: Int)                extends StorageOperation

  object StorageOperation {
    private implicit val getDecoder: Decoder[Get] = deriveDecoder
    private implicit val putDecoder: Decoder[Put] = deriveDecoder
    private implicit val delDecoder: Decoder[Del] = deriveDecoder

    implicit val decoder: Decoder[StorageOperation] = (c: HCursor) =>
      c.downField("type").as[String] match {
        case Right("get") => c.as[Get]
        case Right("put") => c.as[Put]
        case Right("del") => c.as[Del]
        case Right(s)     => DecodingFailure(s"Unknown operation type: $s", c.history).asLeft
        case Left(_)      => DecodingFailure("Type of operation is not specified", c.history).asLeft
    }
  }
}
