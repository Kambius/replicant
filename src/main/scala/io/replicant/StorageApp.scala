package io.replicant

import akka.actor
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator => PubSub}
import akka.cluster.typed.{Cluster, Subscribe}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import io.replicant.StorageManager.{StorageCommand, StorageModificationCommand}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

object ClusterListener {
  val behavior: Behavior[MemberEvent] =
    Behaviors.setup { context =>
      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[MemberEvent])

      Behaviors.receiveMessage { message =>
        context.log.info(s"Cluster event: $message")
        Behaviors.same
      }
    }
}

object DataReplicator {
  sealed trait ReplicationCommand
  final case class Replicate(command: StorageModificationCommand) extends ReplicationCommand

  private sealed trait InternalMsg                                                extends ReplicationCommand
  private final case class ReplicatedCommand(command: StorageModificationCommand) extends InternalMsg
  private final case class Subscribed(s: PubSub.Subscribe)                        extends InternalMsg

  val topic = "replication-topic"

  def behavior(storageManager: ActorRef[StorageCommand]): Behavior[ReplicationCommand] =
    Behaviors.setup { context =>
      val pupSubAdapter = context.messageAdapter[Any] {
        case d: StorageModificationCommand => ReplicatedCommand(d)
        case s: PubSub.SubscribeAck        => Subscribed(s.subscribe)
      }

      val mediator = DistributedPubSub(context.system.toUntyped).mediator

      mediator ! PubSub.Subscribe(topic, pupSubAdapter.toUntyped)

      Behaviors.receiveMessage {
        case Replicate(data) =>
          mediator ! PubSub.Publish(topic, data)
          Behaviors.same

        case ReplicatedCommand(command) =>
          storageManager ! command
          Behaviors.same

        case Subscribed(_) =>
          // todo: retry if no confirmation is send
          Behaviors.same
      }
    }
}

object StorageManager {
  sealed trait StorageResult
  sealed trait StorageDataResult         extends StorageResult
  sealed trait StorageModificationResult extends StorageDataResult
  object StorageResult {
    case object Success                     extends StorageModificationResult
    final case class Data(value: String)    extends StorageDataResult
    final case class Failure(error: String) extends StorageDataResult with StorageModificationResult
  }

  sealed trait StorageCommand
  sealed trait StorageModificationCommand extends StorageCommand
  object StorageCommand {
    final case class Get(
        key: String,
        replyTo: ActorRef[StorageDataResult]
    ) extends StorageCommand

    final case class Put(
        key: String,
        value: String,
        replyTo: ActorRef[StorageModificationResult]
    ) extends StorageModificationCommand

    final case class Del(
        key: String,
        replyTo: ActorRef[StorageModificationResult]
    ) extends StorageModificationCommand
  }

  def behaviorWithReplicator(storage: StorageService): Behavior[StorageCommand] =
    Behaviors.setup { context =>
      val manager    = context.spawn(behavior(storage), "internal-storage-manager")
      val replicator = context.spawn(DataReplicator.behavior(manager), "replicator")

      Behaviors.receiveMessage {
        case c: StorageCommand.Get =>
          manager ! c
          Behaviors.same

        case c: StorageModificationCommand =>
          replicator ! DataReplicator.Replicate(c)
          Behaviors.same
      }
    }

  def behavior(storage: StorageService): Behavior[StorageCommand] =
    Behaviors.receiveMessage {
      case StorageCommand.Get(key, replyTo) =>
        storage.get(key) match {
          case Some(value) => replyTo ! StorageResult.Data(value)
          case None        => replyTo ! StorageResult.Failure("Not found")
        }

        Behaviors.same

      case StorageCommand.Put(key, value, replyTo) =>
        storage.put(key, value)
        replyTo ! StorageResult.Success
        Behaviors.same

      case StorageCommand.Del(key, replyTo) =>
        storage.del(key)
        replyTo ! StorageResult.Success
        Behaviors.same
    }
}

final case class MainConfig(communicationPort: Int, apiPort: Int)

object StorageApp {
  private val logger = LoggerFactory.getLogger(getClass)

  def behavior(config: MainConfig): Behavior[Nothing] =
    Behaviors.setup[Nothing] { context =>
      context.spawn(ClusterListener.behavior, "listener")

      implicit val sys: actor.ActorSystem       = context.system.toUntyped
      implicit val mat: ActorMaterializer       = ActorMaterializer()
      implicit val ec: ExecutionContextExecutor = context.system.executionContext

      val storage = new SqliteStorage(s"./data/stored-data-${config.apiPort}.db")
      val service = new BasicStorageService(storage)

      val storageManager = context.spawn(StorageManager.behaviorWithReplicator(service), "storage-manager")

      val controller = new StorageController(storageManager, context.system)

      val binding = Http().bindAndHandle(controller.route, "localhost", config.apiPort)

      binding.onComplete {
        case Success(_) => context.log.info("Http API started successfully")
        case Failure(e) => context.log.error("Http API start failed", e)
      }

      binding.flatMap(_.whenTerminated).onComplete {
        case Success(_) => context.log.info("Http API terminated successfully")
        case Failure(e) => context.log.error("Http API terminated with failure", e)
      }

      Behaviors.empty[Nothing]
    }

  def startup(configs: Seq[MainConfig]): Unit =
    configs.foreach { c =>
      logger.info(s"Starting app [communication port: ${c.communicationPort}; api port: ${c.apiPort}]")

      val config = ConfigFactory
        .parseString(s"akka.remote.netty.tcp.port=${c.communicationPort}")
        .withFallback(ConfigFactory.load())

      val system = ActorSystem[Nothing](behavior(c), "cluster-system", config)

      system.whenTerminated.onComplete {
        case Success(_) => logger.info("Bue!")
        case Failure(e) => logger.error("Actor system terminated with failure", e)
      }(system.executionContext)
    }

  def main(args: Array[String]): Unit =
    startup(Seq(MainConfig(2551, 9001), MainConfig(2552, 9002), MainConfig(0, 9000)))
}
