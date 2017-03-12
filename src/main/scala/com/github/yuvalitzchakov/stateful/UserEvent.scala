package com.github.yuvalitzchakov.stateful

import argonaut.Argonaut._
import argonaut.CodecJson

/**
  * Created by Yuval.Itzchakov on 3/12/2017.
  */
case class UserEvent(id: Int, data: String, isLast: Boolean)
object UserEvent {
  implicit def codec: CodecJson[UserEvent] =
    casecodec3(UserEvent.apply, UserEvent.unapply)("id", "data", "isLast")

  lazy val empty = UserEvent(-1, "", isLast = false)
}
