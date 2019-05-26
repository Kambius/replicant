package io.replicant.common.instances

import cats.PartialOrder

object vectorclock {
  implicit val partialOrder: PartialOrder[Map[String, Int]] = (x: Map[String, Int], y: Map[String, Int]) => {
    val keys = x.keySet.union(y.keySet)
    if (keys.forall(k => x.getOrElse(k, 0) == y.getOrElse(k, 0))) {
      0
    } else if (keys.forall(k => x.getOrElse(k, 0) >= y.getOrElse(k, 0))) {
      1
    } else if (keys.forall(k => x.getOrElse(k, 0) <= y.getOrElse(k, 0))) {
      -1
    } else {
      Double.NaN
    }
  }
}
