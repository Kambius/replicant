package io.replicant

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

import akka.actor
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import kamon.Kamon
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.io.Source
import scala.util.{Failure, Random, Success}

final case class MainConfig(communicationPort: Int, apiPort: Int, hashingLabel: Int)

object MainConfig {
  def init(communicationPort: Int, apiPort: Int, hashingFile: String): MainConfig =
    if (Files.exists(Paths.get(hashingFile))) {
      val s            = Source.fromFile(hashingFile, "UTF-8")
      val hashingLabel = s.mkString.toInt
      s.close()
      MainConfig(communicationPort, apiPort, hashingLabel)
    } else {
      println("1" * 30)
      val hashingLabel = Random.nextInt()
      val w            = new PrintWriter(new File(hashingFile))
      w.print(hashingLabel)
      w.close()
      MainConfig(communicationPort, apiPort, hashingLabel)
    }
}

object StorageApp {
  private val logger = LoggerFactory.getLogger(getClass)

  final val Name: String = "cluster-system"

  def behavior(config: MainConfig): Behavior[Nothing] =
    Behaviors.setup[Nothing] { ctx =>
      ctx.spawn(ClusterListener.behavior, "listener")

      implicit val sys: actor.ActorSystem       = ctx.system.toUntyped
      implicit val mat: ActorMaterializer       = ActorMaterializer()
      implicit val ec: ExecutionContextExecutor = ctx.system.executionContext

      val storage = new EmbeddedDbStorage(s"./data/stored-data-${config.apiPort}")

      val storageManager = ctx.spawn(StorageManager.behavior, "storage-manager")

      ctx.spawn(ReplicationManager.behavior(storage), ReplicationManager.Name)

      val controller = new StorageController(storageManager, ctx.system)

      val binding = Http().bindAndHandle(controller.route, "localhost", config.apiPort)

      binding.onComplete {
        case Success(_) => ctx.log.info("Http API started successfully")
        case Failure(e) => ctx.log.error("Http API start failed", e)
      }

      binding.flatMap(_.whenTerminated).onComplete {
        case Success(_) => ctx.log.info("Http API terminated successfully")
        case Failure(e) => ctx.log.error("Http API terminated with failure", e)
      }

      Behaviors.empty[Nothing]
    }

  def startup(configs: Seq[MainConfig]): Unit =
    configs.foreach { c =>
      logger.info(s"Starting app [communication port: ${c.communicationPort}; api port: ${c.apiPort}]")

      val config = ConfigFactory
        .parseString(s"""akka.remote.netty.tcp.port = ${c.communicationPort}
                        |akka.cluster.roles = ["${c.hashingLabel}"]""".stripMargin)
        .withFallback(ConfigFactory.load())

      val system = ActorSystem[Nothing](behavior(c), Name, config)

      system.whenTerminated.onComplete {
        case Success(_) => logger.info("Bye!")
        case Failure(e) => logger.error("Actor system terminated with failure", e)
      }(system.executionContext)
    }

  def main(args: Array[String]): Unit = {
    Kamon.loadReportersFromConfig()
    startup(
      Seq(MainConfig.init(2551, 9001, "data/hash-9001.dat"),
          MainConfig.init(2552, 9002, "data/hash-9002.dat"),
          MainConfig.init(2553, 9000, "data/hash-9000.dat"))
    )
  }
}
