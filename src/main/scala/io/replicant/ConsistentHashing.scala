package io.replicant

import akka.cluster.{Member, MemberStatus}

object ConsistentHashing {
  def getReplicaSet(members: Set[Member], replicationLevel: Int, key: String): Set[Member] = {
    val sorted = members
      .filter(_.status == MemberStatus.Up)
      .toSeq
      .flatMap(m => m.roles.head.split(",").map(_.toInt).map(m -> _))
      .sortBy(_._2)

    val idx = sorted.reverse.indexWhere(_._2 <= key.##)

    val fixedIdx = if (idx == -1) sorted.size - 1 else sorted.size - idx - 1

    (sorted.drop(fixedIdx) ++ sorted.take(fixedIdx + 1))
      .map(_._1)
      .scanLeft(Set.empty[Member])(_ + _)
      .find(_.size == replicationLevel)
      .get
  }
}
