package net.learningclub.dstreamsredis

import com.redis.RedisClientPool
import io.circe.parser.decode
import io.circe.syntax._
import net.learningclub._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Minutes, Seconds}

object EventsProcessorRedis {

  private val redisClientPool = new RedisClientPool(host = "localhost", port = 6379)

  def evaluateAdEvents(input: DStream[AdEvent]): DStream[List[EvaluatedAdEvent]] = {
    input
      .map(event => (HostIp(event.ip), List(event)))
      .reduceByKeyAndWindow(
        (events: List[AdEvent], otherEvents: List[AdEvent]) => events ++ otherEvents,
        (events, oldEvents) => events diff oldEvents,
        Seconds(10),
        Seconds(10)
      )
      .map { case (hostIp, newEvents) =>
        val ip = hostIp.ip
        val newClickTimes = newEvents.foldLeft(List.empty[Long])((currAgg, nextEvent) => {
          nextEvent.event_time :: currAgg
        })

        val state = redisClientPool.withClient(c => c.hgetall("hosts")).getOrElse(Map.empty[String, String])
          .mapValues(decode[HostState](_).right.toOption)
          .filter { case (_, hostStateOpt) => hostStateOpt.isDefined }
          .mapValues(_.get)
        val stateWithCurrentHostUpdated = state.get(ip) match {
          case Some(currHostState) =>
            val currentClickTimes = currHostState.recentClicks
            val clicksWithNew = newClickTimes ::: currentClickTimes
            state.updated(ip, calculateNewHostStateForAllWindows(clicksWithNew))

          case None => state + (ip -> calculateNewHostStateForAllWindows(newClickTimes))
        }

        // whitelisting hosts after 10 minutes over last click time bot detection condition not matches
        val updatedState = stateWithCurrentHostUpdated
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
        val updatedStateStr = updatedState.mapValues(_.asJson.noSpaces)

        redisClientPool.withClient(c => c.hmset("hosts", updatedStateStr))

        newEvents.map(event => {
          val isBot = updatedState.get(ip).exists(_.botDetection.isBot)
          EvaluatedAdEvent(event, isBot)
        })
      }
      .filter(_.nonEmpty)
  }

}
