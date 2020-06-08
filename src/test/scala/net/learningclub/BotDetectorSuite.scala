package net.learningclub

import java.util.concurrent.TimeUnit

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.redis.RedisClientPool
import net.learningclub.dstreamsredis.EventsProcessorRedis
import net.learningclub.statefuldstreams.EventsProcessorStateful
import net.learningclub.util.EventsGenerator
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.{SparkConf, streaming}
import org.scalatest.{BeforeAndAfter, WordSpec}

import scala.concurrent.duration.Duration

class BotDetectorSuite extends WordSpec with BeforeAndAfter with StreamingSuiteBase {

  private val redisClientPool: RedisClientPool = new RedisClientPool("localhost", 6379)

  "DStreams - stateful streaming" should {
    "not detect bots - expected" in {
      // GIVEN
      val input1 = EventsGenerator.generateAdEvents(1).toList
      val input2 = EventsGenerator.generateAdEvents(1).toList
      val input3 = EventsGenerator.generateAdEvents(1).toList

      val input = List(
        input1,
        input2,
        input3
      )
      val output = List(
        List(List(EvaluatedAdEvent(input1.head, isBot = false))),
        List(List(EvaluatedAdEvent(input2.head, isBot = false))),
        List(List(EvaluatedAdEvent(input3.head, isBot = false)))
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorStateful.evaluateAdEvents, output, ordered = false)
    }

    "detect bots within a window" in {
      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 21,
        botsNumberOpt = Some(20),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl)
      ).toList

      val input = List(
        events1
      )

      val (bots, allowed) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedBots = bots.map(b => EvaluatedAdEvent(b, isBot = true))
      val evaluatedNormal = allowed.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        List(
          evaluatedBots,
          evaluatedNormal
        )
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorStateful.evaluateAdEvents, output, ordered = false)
    }

    "detect bots within the 2nd window" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 21 events with 10 bot clicks
      // * bot clicks should be evaluated as normal since the number is 10
      // The 2nd:
      // * 30 events with 10 bot clicks
      // * bot clicks should be evaluated as bot since the total number of bots is 20 and interval is 3 seconds

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      // First part of bots for the 1st window
      val botTime1 = System.currentTimeMillis() - Duration(5, TimeUnit.SECONDS).toMillis
      // Second part of bots 3 seconds later for the 2nd window
      val botTime2 = botTime1 + Duration(3, TimeUnit.SECONDS).toMillis

      // 10 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 10,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).toList

      // 10 bots in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 10,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList

      val input = List(
        events1,
        events2
      )

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      // First part of bot clicks should be treated as allowed since the number is 10
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = false))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be treated as bots since the total number is 20
      val evaluatedBots_2 = bots2.map(b => EvaluatedAdEvent(b, isBot = true))
      val evaluatedNormal_2 = allowed2.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        // 1st output
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e)),
        // 2nd output
        evaluatedBots_2 :: evaluatedNormal_2.map(e => List(e)),
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorStateful.evaluateAdEvents, output, ordered = false)
    }

    "should not detect bots within the 2nd window since the interval is more than 10 seconds" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 21 events with 10 bot clicks
      // * bot clicks should be evaluated as normal since the number is 10
      // The 2nd:
      // * 30 events with 10 bot clicks but the interval is 15 seconds
      // * bot clicks should be evaluated as allowed

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      // First part of bots for the 1st window
      val botTime1 = System.currentTimeMillis() - Duration(10, TimeUnit.SECONDS).toMillis
      // Second part of bots 3 seconds later for the 2nd window
      val botTime2 = botTime1 + Duration(15, TimeUnit.SECONDS).toMillis

      // 10 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 21,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).toList

      // 10 bots in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 30,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList

      val input = List(
        events1,
        events2
      )

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      // First part of clicks should be treated as allowed since the number is 10
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = false))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be also treated as allowed since the interval is 15 seconds
      val evaluatedBots_2 = bots2.map(b => EvaluatedAdEvent(b, isBot = false))
      val evaluatedNormal_2 = allowed2.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        // 1st output
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e)),
        // 2nd output
        evaluatedBots_2 :: evaluatedNormal_2.map(e => List(e)),
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorStateful.evaluateAdEvents, output, ordered = false)
    }

    "detect late bots within the 2nd window" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 21 events with 10 old bot clicks (9 bots are 7 min late and 1 bot is 7 min - 5 sec late)
      // * bot clicks should be evaluated as normal since the number is 10
      // The 2nd:
      // * 30 events with 10 old bot clicks (all are 7 min - 5 sec late)
      // * bot clicks should be evaluated as bot since the total number of bots is 20 and belong to the same 10 sec interval

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val currentTime = System.currentTimeMillis()
      // First part of bots for the 1st window - 7 min late
      val botTime1 = currentTime - Duration(7, TimeUnit.MINUTES).toMillis
      // Second part of bots for the 2nd window - 7 min - 5 иусщтви late
      val botTime2 = botTime1 + Duration(5, TimeUnit.SECONDS).toMillis

      // 10 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 1,
        botsNumberOpt = Some(1),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList.head ::
      EventsGenerator.generateAdEvents(
        eventsNumber = 21,
        botsNumberOpt = Some(9),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).toList

      // 10 bots in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 30,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList

      val input = List(
        events1,
        events2
      )

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = false))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedBots_2 = bots2.map(b => EvaluatedAdEvent(b, isBot = true))
      val evaluatedNormal_2 = allowed2.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        // 1st output
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e)),
        // 2nd output
        evaluatedBots_2 :: evaluatedNormal_2.map(e => List(e)),
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorStateful.evaluateAdEvents, output, ordered = false)
    }

    "ignore clicks older 10 minutes within the same window for a specific host" in {
      // Scenario
      // * 26 events within the same window - all are bots but 25 events are older 10 minutes
      // * bot clicks should be evaluated as normal since 25 are old

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      // First part of bots for the 1st window
      val currentClick = System.currentTimeMillis()
      val oldBotsTime = currentClick - Duration(11, TimeUnit.MINUTES).toMillis

      // 10 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 25,
        botsNumberOpt = Some(25),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(oldBotsTime)
      ).toList

      val event2 = EventsGenerator.generateAdEvents(
        eventsNumber = 1,
        botsNumberOpt = Some(1),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(currentClick)
      ).toList.head

      val resultEvents = event2 :: events1

      val input = List(
        resultEvents
      )

      val (bots1, allowed1) = resultEvents.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      // bot clicks should be treated as allowed even though the number > 20 since they older 10 minutes
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = false))

      val output = List(
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e))
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorStateful.evaluateAdEvents, output, ordered = false)
    }

    "whitelist a host after 10 minutes a bot detection condition not matches" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 30 events with 25 bot clicks
      // * bot clicks should be evaluated as bot
      // The 2nd:
      // * 30 events with 1 bot click (11 minutes after ones in the 1st window)
      // * bot host should be whitelisted

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val currentTime = System.currentTimeMillis()
      // First part of bots for the 1st window
      val botTime1 = currentTime - Duration(9, TimeUnit.MINUTES).toMillis
      // Second part of bots for the 2nd window
      val botTime2 = botTime1 + Duration(11, TimeUnit.MINUTES).toMillis

      // 25 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 30,
        botsNumberOpt = Some(25),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).toList

      // 1 bot in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 30,
        botsNumberOpt = Some(1),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList

      val input = List(
        events1,
        events2
      )

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      // First part of bot clicks should be evaluated as bots
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = true))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be evaluated as allowed since bot detection condition not matches more than 10 minutes
      val evaluatedBots_2 = bots2.map(b => EvaluatedAdEvent(b, isBot = false))
      val evaluatedNormal_2 = allowed2.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        // 1st output
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e)),
        // 2nd output
        evaluatedBots_2 :: evaluatedNormal_2.map(e => List(e)),
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorStateful.evaluateAdEvents, output, ordered = false)
    }

    "not whitelist a host after 9 minutes a bot detection condition not matches" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 30 events with 25 bot clicks
      // * bot clicks should be evaluated as bot
      // The 2nd:
      // * 30 events with 1 bot click (9 minutes after ones in the 1st window)
      // * bot host should be whitelisted

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val currentTime = System.currentTimeMillis()
      // First part of bots for the 1st window
      val botTime1 = currentTime - Duration(9, TimeUnit.MINUTES).toMillis
      // Second part of bots for the 2nd window
      val botTime2 = currentTime

      // 25 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 30,
        botsNumberOpt = Some(25),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).toList

      // 1 bot in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 30,
        botsNumberOpt = Some(1),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList

      val input = List(
        events1,
        events2
      )

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      // First part of bot clicks should be evaluated as bots
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = true))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be evaluated as allowed since bot detection condition not matches more than 10 minutes
      val evaluatedBots_2 = bots2.map(b => EvaluatedAdEvent(b, isBot = true))
      val evaluatedNormal_2 = allowed2.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        // 1st output
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e)),
        // 2nd output
        evaluatedBots_2 :: evaluatedNormal_2.map(e => List(e)),
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorStateful.evaluateAdEvents, output, ordered = false)
    }
  }

  "DStreams - streaming with Redis state storage" should {

    before {
      redisClientPool.withClient(c => c.flushall)
    }

    after {
      redisClientPool.withClient(c => c.flushall)
    }

    "detect bots within the 2nd window" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 21 events with 10 bot clicks
      // * bot clicks should be evaluated as normal since the number is 10
      // The 2nd:
      // * 30 events with 10 bot clicks
      // * bot clicks should be evaluated as bot since the total number of bots is 20 and interval is 3 seconds

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      // First part of bots for the 1st window
      val botTime1 = System.currentTimeMillis() - Duration(5, TimeUnit.SECONDS).toMillis
      // Second part of bots 3 seconds later for the 2nd window
      val botTime2 = botTime1 + Duration(3, TimeUnit.SECONDS).toMillis

      // 10 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 21,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).toList

      // 10 bots in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 30,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList

      val input = List(
        events1,
        events2
      )

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      // First part of bot clicks should be treated as allowed since the number is 10
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = false))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be treated as bots since the total number is 20
      val evaluatedBots_2 = bots2.map(b => EvaluatedAdEvent(b, isBot = true))
      val evaluatedNormal_2 = allowed2.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        // 1st output
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e)),
        // 2nd output
        evaluatedBots_2 :: evaluatedNormal_2.map(e => List(e)),
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorRedis.evaluateAdEvents, output, ordered = false)
    }

    "should not detect bots within the 2nd window since the interval is more than 10 seconds" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 21 events with 10 bot clicks
      // * bot clicks should be evaluated as normal since the number is 10
      // The 2nd:
      // * 30 events with 10 bot clicks but the interval is 15 seconds
      // * bot clicks should be evaluated as allowed

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      // First part of bots for the 1st window
      val botTime1 = System.currentTimeMillis() - Duration(10, TimeUnit.SECONDS).toMillis
      // Second part of bots 3 seconds later for the 2nd window
      val botTime2 = botTime1 + Duration(15, TimeUnit.SECONDS).toMillis

      // 10 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 21,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).toList

      // 10 bots in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 30,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList

      val input = List(
        events1,
        events2
      )

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      // First part of clicks should be treated as allowed since the number is 10
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = false))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be also treated as allowed since the interval is 15 seconds
      val evaluatedBots_2 = bots2.map(b => EvaluatedAdEvent(b, isBot = false))
      val evaluatedNormal_2 = allowed2.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        // 1st output
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e)),
        // 2nd output
        evaluatedBots_2 :: evaluatedNormal_2.map(e => List(e)),
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorRedis.evaluateAdEvents, output, ordered = false)
    }

    "whitelist a host after 10 minutes a bot detection condition not matches" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 30 events with 25 bot clicks
      // * bot clicks should be evaluated as bot
      // The 2nd:
      // * 30 events with 1 bot click (11 minutes after ones in the 1st window)
      // * bot host should be whitelisted

      // GIVEN
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val currentTime = System.currentTimeMillis()
      // First part of bots for the 1st window
      val botTime1 = currentTime - Duration(9, TimeUnit.MINUTES).toMillis
      // Second part of bots for the 2nd window
      val botTime2 = botTime1 + Duration(11, TimeUnit.MINUTES).toMillis

      // 25 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 25,
        botsNumberOpt = Some(25),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).toList

      // 1 bot in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 1,
        botsNumberOpt = Some(1),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).toList

      val input = List(
        events1,
        events2
      )

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val evaluatedNormal_1 = allowed1.map(e => EvaluatedAdEvent(e, isBot = false))
      // First part of bot clicks should be evaluated as bots
      val evaluatedBots_1 = bots1.map(b => EvaluatedAdEvent(b, isBot = true))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be evaluated as allowed since bot detection condition not matches more than 10 minutes
      val evaluatedBots_2 = bots2.map(b => EvaluatedAdEvent(b, isBot = false))
      val evaluatedNormal_2 = allowed2.map(e => EvaluatedAdEvent(e, isBot = false))

      val output = List(
        // 1st output
        evaluatedBots_1 :: evaluatedNormal_1.map(e => List(e)),
        // 2nd output
        evaluatedBots_2 :: evaluatedNormal_2.map(e => List(e)),
      )

      // WHEN -> THEN
      testOperation(input, EventsProcessorRedis.evaluateAdEvents, output, ordered = false)
    }
  }

  override def batchDuration: streaming.Duration = org.apache.spark.streaming.Duration(Seconds(10).milliseconds)

  override def conf: SparkConf = new SparkConf()
    // default of test framework
    .setMaster(master)
    .setAppName(framework)
    .set("spark.driver.host", "localhost")
    .set("spark.streaming.clock", "org.apache.spark.streaming.util.TestManualClock")
    // custom
    .set("spark.driver.memory", "2g")
    .set("spark.broadcast.compress", "false")
    .set("spark.shuffle.compress", "false")
    .set("spark.shuffle.spill.compress", "false")

  override def maxWaitTimeMillis: Int = org.apache.spark.streaming.Duration(Minutes(1).milliseconds).milliseconds.toInt
}
