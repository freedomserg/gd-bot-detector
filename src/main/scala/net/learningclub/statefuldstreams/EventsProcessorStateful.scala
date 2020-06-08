package net.learningclub.statefuldstreams

import net.learningclub._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

object EventsProcessorStateful {

  def evaluateAdEvents(input: DStream[AdEvent]): DStream[List[EvaluatedAdEvent]] = {
    input
      .map(event => (HostIp(event.ip), List(event)))
      .reduceByKeyAndWindow(
        (events: List[AdEvent], otherEvents: List[AdEvent]) => events ++ otherEvents,
        (events, oldEvents) => events diff oldEvents,
        Seconds(10),
        Seconds(10)
      )
      .mapWithState(
        StateSpec.function((hostIp: HostIp, newEventsOpt: Option[List[AdEvent]],
                            oldState: State[Map[String, HostState]]) => {
          val ip = hostIp.ip
          val newEvents = newEventsOpt.getOrElse(List.empty[AdEvent])
          val newClickTimes = newEvents.foldLeft(List.empty[Long])((currAgg, nextEvent) => {
            nextEvent.event_time :: currAgg
          })

          val oldStateOpt = oldState.getOption()

          val newState = oldStateOpt match {
            case Some(currState) =>
              val updatedState = currState.get(ip) match {
                case Some(currentHostState) =>
                  val currentClickTimes = currentHostState.recentClicks
                  val clicksWithNew = newClickTimes ::: currentClickTimes
                  val newHostState = calculateNewHostStateForAllWindows(clicksWithNew)
                  currState.updated(ip, newHostState)
                case None =>
                  currState + (ip -> calculateNewHostStateForAllWindows(newClickTimes))
              }
              updatedState
                // whitelisting hosts after 10 minutes over last click time bot detection condition not matches
                .mapValues(hostState => {
                  val botDetection = hostState.botDetection
                  val lastClickTime = hostState.recentClicks.max
                  if (botDetection.isBot && botDetection.detectionTime.exists(_ <= (lastClickTime - Minutes(10).milliseconds))) {
                    HostState(hostState.recentClicks, BotDetection())
                  } else hostState
                })
                .filter { case (_, hostState) => if (hostState.botDetection.isBot) true
                  else hostState.recentClicks.nonEmpty
                }
            case None => Map(ip -> calculateNewHostStateForAllWindows(newClickTimes))
          }
          oldState.update(newState)

          newEvents.map(event => {
            val isBot = newState.get(event.ip)
              .exists(_.botDetection.isBot)
            EvaluatedAdEvent(event, isBot)
          })
        })
      )
      .filter(_.nonEmpty)
  }

}
