package io.replicant.common

import akka.actor.typed.scaladsl.ActorContext
import akka.cluster.typed.Cluster

object KamonUtil {
  def mkNodeTag(ctx: ActorContext[_]): (String, String) = {
    val member = Cluster(ctx.system).selfMember
    "node" -> (member.address.host.getOrElse("") + ":" + member.address.port.getOrElse(""))
  }
}
