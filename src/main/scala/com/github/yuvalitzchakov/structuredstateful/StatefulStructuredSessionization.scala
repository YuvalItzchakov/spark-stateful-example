package com.github.yuvalitzchakov.structuredstateful

import com.github.yuvalitzchakov.user.{UserEvent, UserSession}
import org.apache.spark.sql._

import scalaz.{-\/, \/-}
import argonaut.Argonaut._
import org.apache.spark.sql.streaming.{
  GroupState,
  GroupStateTimeout,
  OutputMode
}

/**
  * Created by Yuval.Itzchakov on 29/07/2017.
  */
object StatefulStructuredSessionization {
  implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
  implicit val userSessionEncoder: Encoder[Option[UserSession]] =
    Encoders.kryo[Option[UserSession]]

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: SparkRunner <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Stateful Structured Streaming")
      .getOrCreate()

    import spark.implicits._

    val userEventsStream: Dataset[String] = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()
      .as[String]

    val finishedUserSessionsStream: Dataset[UserSession] =
      userEventsStream
        .map(deserializeUserEvent)
        .groupByKey(_.id)
        .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(
          updateSessionEvents)
        .flatMap(userSession => userSession)


    finishedUserSessionsStream.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("checkpointLocation", "c:\\temp\\structured")
      .start()
      .awaitTermination()
  }

  def updateSessionEvents(
      id: Int,
      userEvents: Iterator[UserEvent],
      state: GroupState[UserSession]): Option[UserSession] = {
    if (state.hasTimedOut) {
      // We've timed out, lets extract the state and send it down the stream
      state.remove()
      state.getOption
    } else {
      /*
       New data has come in for the given user id. We'll look up the current state
       to see if we already have something stored. If not, we'll just take the current user events
       and update the state, otherwise will concatenate the user events we already have with the
       new incoming events.
       */
      val currentState = state.getOption
      val updatedUserSession =
        currentState.fold(UserSession(userEvents.toSeq))(currentUserSession =>
          UserSession(currentUserSession.userEvents ++ userEvents.toSeq))
      state.update(updatedUserSession)

      if (updatedUserSession.userEvents.exists(_.isLast)) {
        /*
         If we've received a flag indicating this should be the last event batch, let's close
         the state and send the user session downstream.
         */
        val userSession = state.getOption
        state.remove()
        userSession
      } else {

        state.setTimeoutDuration("1 minute")

        None
      }
    }
  }

  def deserializeUserEvent(json: String): UserEvent = {
    json.decodeEither[UserEvent] match {
      case \/-(userEvent) => userEvent
      case -\/(error) =>
        println(s"Failed to parse user event: $error")
        UserEvent.empty
    }
  }
}
