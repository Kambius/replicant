package io.replicant

import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.typed.{Cluster, Subscribe}
import com.typesafe.config.ConfigFactory

object HelloWorld {
  final case class Greet(whom: String, replyTo: ActorRef[Greeted])
  final case class Greeted(whom: String, from: ActorRef[Greet])

  val greeter: Behavior[Greet] = Behaviors.receive { (context, message) =>
    context.log.info("Hello {}!", message.whom)
    message.replyTo ! Greeted(message.whom, context.self)
    Behaviors.same
  }
}

object HelloWorldBot {
  def bot(greetingCounter: Int, max: Int): Behavior[HelloWorld.Greeted] =
    Behaviors.receive { (context, message) =>
      val n = greetingCounter + 1
      context.log.info("Greeting {} for {}", n, message.whom)
      if (n == max) {
        Behaviors.stopped
      } else {
        message.from ! HelloWorld.Greet(message.whom, context.self)
        bot(n, max)
      }
    }
}

object SimpleClusterListener {
  val listener: Behavior[MemberEvent] =
    Behaviors.setup { context =>
      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[MemberEvent])

      Behaviors.receiveMessage { message =>
        context.log.info(s"CLUSTER EVENT: $message")
        Behaviors.same
      }
    }
}

object HelloWorldMain {
  final case class Start(name: String)

  val main: Behavior[Start] =
    Behaviors.setup { context =>
      val greeter = context.spawn(HelloWorld.greeter, "greeter")

      context.spawn(SimpleClusterListener.listener, "listener")

      Behaviors.receiveMessage { message =>
        val replyTo = context.spawn(HelloWorldBot.bot(greetingCounter = 0, max = 3), message.name)
        greeter ! HelloWorld.Greet(message.name, replyTo)
        Behaviors.same
      }
    }
}

object SimpleClusterApp {
  def main(args: Array[String]): Unit =
    startup(Seq(2551, 2552, 0))

  def startup(ports: Seq[Int]): Unit =
    ports.foreach { port =>
      val config = ConfigFactory
        .parseString(s"akka.remote.netty.tcp.port=$port")
        .withFallback(ConfigFactory.load())

      val system = ActorSystem(HelloWorldMain.main, "cluster-system", config)

      system ! HelloWorldMain.Start("World")
    }
}
