package io.replicant

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import io.replicant.common.KamonUtil
import kamon.Kamon

import scala.concurrent.duration._

object StatsActor {
  private final val Timer: String = "timer"

  final val Name: String = "stats-actor"

  case object Tick
  final val Interval: FiniteDuration = 1.second

  def behavior(storage: Storage): Behavior[Tick.type] =
    Behaviors.withTimers { scheduler =>
      scheduler.startPeriodicTimer(Timer, Tick, Interval)

      Behaviors.receive {
        case (ctx, Tick) =>
          Kamon.gauge("entries").refine(KamonUtil.mkNodeTag(ctx)).set(storage.count())
          Behaviors.same
      }
    }
}
