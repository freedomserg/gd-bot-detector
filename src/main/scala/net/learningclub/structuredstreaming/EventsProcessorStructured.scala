package net.learningclub.structuredstreaming

import net.learningclub._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.streaming.Minutes

case class HostStatistics(hostIp: HostIp, state: Map[Window, HostState])

object EventsProcessorStructured {


  def evaluateAdEvents(input: Dataset[AdEventWithID]): Dataset[EvaluatedAdEventT] = {

    def updateHostStatistics(hostIp: HostIp,
                              newEvents: Iterator[AdEventWithWindow],
                              oldState: GroupState[HostStatistics]): List[EvaluatedAdEventT] = {
      def calculateStateDelta(events: List[AdEventWithWindow]): Map[Window, HostState] = {
        events.groupBy(_.window)
          .mapValues(events => {
            val clicksInWindow = events.map(_.event_time.getTime)
            calculateNewHostStateForSingleWindow(clicksInWindow)
          })
      }

      if (!oldState.hasTimedOut) {
        val input = newEvents.toList
        val currentStateOpt = oldState.getOption
        val stateDelta = calculateStateDelta(input)

        val newHostStat = currentStateOpt match {
          case Some(currentHostStat) =>
            val currentHostState = currentHostStat.state
            val updatedHostState = stateDelta.foldLeft(currentHostState)((accum, next) => {
              val window = next._1
              val newHostStateForWindow = next._2
              accum.get(window) match {
                case Some(currentHostStateForWindow) =>
                  val totalClicksInWindow = newHostStateForWindow.recentClicks ::: currentHostStateForWindow.recentClicks
                  val updatedHostStateForWindow = calculateNewHostStateForSingleWindow(totalClicksInWindow)
                  accum.updated(window, updatedHostStateForWindow)
                case None => accum + (window -> newHostStateForWindow)
              }
            })
            HostStatistics(hostIp, updatedHostState)

          case None => HostStatistics(hostIp, stateDelta)

        }
        // whitelisting hosts after 10 minutes over last click time bot detection condition not matches
        val lastSeenClick = newHostStat.state.values.flatMap(_.recentClicks).max
        val filteredNewHostState = newHostStat.state
          .mapValues(hostState => {
            val botDetection = hostState.botDetection
            if (botDetection.isBot && botDetection.detectionTime.exists(_ <= (lastSeenClick - Minutes(10).milliseconds))) {
              HostState(hostState.recentClicks, BotDetection())
            } else hostState
          })
          .filter { case (_, hostState) => if (hostState.botDetection.isBot) true
          else hostState.recentClicks.nonEmpty
          }

        val updatedHostStat = newHostStat.copy(state = filteredNewHostState)
        oldState.update(updatedHostStat)

        // Expire state after 10 minutes after last seen click
        oldState.setTimeoutTimestamp(lastSeenClick, "10 minutes")

        input.map(e => {
          val isBot = updatedHostStat.state.exists { case (_, hostState) => hostState.botDetection.isBot }
          EvaluatedAdEventT(e.toAdEventWithID, isBot)
        })
      } else {
        oldState.remove()
        Nil
      }
    }

    val spark = input.sparkSession
    import spark.implicits._

    input
      .withColumn("event_time", ($"event_time" / 1000).cast(TimestampType))
      .withWatermark("event_time", "10 minutes")
      .withColumn("window", window($"event_time", "10 seconds", "10 seconds"))
      .as[AdEventWithWindow]
      .groupByKey(r => HostIp(r.ip))
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(updateHostStatistics)
      .flatMap(identity)
  }

}
