package io.replicant

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.typed.{Cluster, Subscribe}

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