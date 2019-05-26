package io.replicant.common.syntax

import io.circe.Decoder
import io.circe.parser._

object json {
  implicit class RichString(val value: String) extends AnyVal {
    def unsafeAs[T: Decoder]: T = parse(value).flatMap(_.as[T]).fold(e => throw e, identity)
  }
}
