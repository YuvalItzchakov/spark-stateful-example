package com.github.yuvalitzchakov.stateful

import com.github.yuvalitzchakov.stateful.configuration.SparkConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import pureconfig._
import argonaut.Argonaut._
import com.github.yuvalitzchakov.user.{UserEvent, UserSession}

import scalaz.{-\/, \/-}

/**
  * Created by Yuval.Itzchakov on 3/12/2017.
  */
object SparkStatefulRunner {
  /**
    * Aggregates User Sessions using Stateful Streaming transformations.
    *
    * Usage: SparkStatefulRunner <hostname> <port>
    * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SparkRunner <hostname> <port>")
      System.exit(1)
    }

    val sparkConfig = loadConfigOrThrow[SparkConfiguration]("spark")

    val sparkContext = new SparkContext(sparkConfig.sparkMasterUrl, "Spark Stateful Streaming")
    val ssc = new StreamingContext(sparkContext, Milliseconds(4000))
    ssc.checkpoint(sparkConfig.checkpointDirectory)

    val stateSpec =
      StateSpec
        .function(updateUserEvents _)
        .timeout(Minutes(sparkConfig.timeoutInMinutes))

    ssc
      .socketTextStream(args(0), args(1).toInt)
      .map(deserializeUserEvent)
      .filter(_ != UserEvent.empty)
      .mapWithState(stateSpec)
      .foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          rdd.foreach(maybeUserSession => maybeUserSession.foreach {
            userSession =>
              // Store user session here
              println(userSession)
          })
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }

  def deserializeUserEvent(json: String): (Int, UserEvent) = {
    json.decodeEither[UserEvent] match {
      case \/-(userEvent) =>
        (userEvent.id, userEvent)
      case -\/(error) =>
        println(s"Failed to parse user event: $error")
        (UserEvent.empty.id, UserEvent.empty)
    }
  }

  def updateUserEvents(key: Int,
                       value: Option[UserEvent],
                       state: State[UserSession]): Option[UserSession] = {
    def updateUserSessions(newEvent: UserEvent): Option[UserSession] = {
      val existingEvents: Seq[UserEvent] =
        state
          .getOption()
          .map(_.userEvents)
          .getOrElse(Seq[UserEvent]())

      val updatedUserSessions = UserSession(newEvent +: existingEvents)

      updatedUserSessions.userEvents.find(_.isLast) match {
        case Some(_) =>
          state.remove()
          Some(updatedUserSessions)
        case None =>
          state.update(updatedUserSessions)
          None
      }
    }

    value match {
      case Some(newEvent) => updateUserSessions(newEvent)
      case _ if state.isTimingOut() => state.getOption()
    }
  }
}
