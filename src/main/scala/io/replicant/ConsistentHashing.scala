package io.replicant

import akka.cluster.{Member, MemberStatus}

object ConsistentHashing {
  def getReplicaSet(members: Set[Member], replicationLevel: Int, key: String): Set[Member] = {
    val sorted = members
      .filter(_.status == MemberStatus.Up)
      .toSeq
      .map(m => m -> m.roles.head.toInt)
      .sortBy(_._2)(Ordering[Int].reverse)

    val idx = sorted.indexWhere(_._2 <= key.##)

    val fixedIdx = if (idx == -1) sorted.size - 1 else idx

    (sorted.slice(fixedIdx, fixedIdx + replicationLevel) ++ sorted.take(fixedIdx + replicationLevel - sorted.size))
      .map(_._1)
      .toSet
  }
}
