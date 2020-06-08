package net.learningclub

import java.util.UUID
import java.util.concurrent.TimeUnit

import net.learningclub.structuredstreaming.EventsProcessorStructured
import net.learningclub.util.EventsGenerator
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.scalatest.WordSpec

import scala.concurrent.duration.Duration

class BotDetectorStructuredStreamingSuite extends WordSpec with Logging {

  val appName: String = "BotDetector"
  var query: StreamingQuery = _

  val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName(appName)
    .config("spark.driver.memory", "2g")
    .config("spark.broadcast.compress", "false")
    .config("spark.shuffle.compress", "false")
    .config("spark.shuffle.spill.compress", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  implicit val sqlCtx: SQLContext = spark.sqlContext

  val queryName = "Evaluated" // table to keep evaluated events

  "Structured streaming" should {

    "not detect bots - expected" in {
      // GIVEN
      val eventsStream = MemoryStream[AdEventWithID]
      val eventsTypedStream: Dataset[AdEventWithID] = eventsStream.toDS()
      val evaluatedEventsStream = EventsProcessorStructured.evaluateAdEvents(eventsTypedStream)

      query = evaluatedEventsStream.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode(OutputMode.Update())
        .start()

      // 1st batch with 1 event
      val initialBatch = EventsGenerator.generateAdEvents(1).map(e =>
        toAdEventWithID(e))

      // WHEN
      eventsStream.addData(initialBatch)
      processDataWithLock7(query)

      // THEN
      val resultBatch1DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection1 = resultBatch1DS.collect()
      val expectedEvent1 = EvaluatedAdEventT(initialBatch.head, isBot = false)
      assert(resultEventsCollection1.length == 1)
      assertResult(expectedEvent1)(resultEventsCollection1.head)

      // 2nd batch with 1 event
      val additionalBatch = EventsGenerator.generateAdEvents(1).map(e =>
        toAdEventWithID(e))

      // WHEN
      eventsStream.addData(additionalBatch)
      processDataWithLock7(query)

      //THEN
      val resultBatch2DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection2 = resultBatch2DS.collect()
      val expectedEvent2 = EvaluatedAdEventT(additionalBatch.head, isBot = false)
      assert(resultEventsCollection2.length == 2)
      assert(resultEventsCollection2.contains(expectedEvent1))
      assert(resultEventsCollection2.contains(expectedEvent2))

      // Clean-up the context
      cleanContext()
    }

    "detect bots within a window" in {
      // GIVEN
      val eventsStream = MemoryStream[AdEventWithID]
      val eventsTypedStream: Dataset[AdEventWithID] = eventsStream.toDS()
      val evaluatedEventsStream = EventsProcessorStructured.evaluateAdEvents(eventsTypedStream)

      query = evaluatedEventsStream.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode(OutputMode.Update())
        .start()

      // Batch with 21 events - 20 bots
      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val initialBatch = EventsGenerator.generateAdEvents(
        eventsNumber = 21,
        botsNumberOpt = Some(20),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl)
      ).map(e => toAdEventWithID(e))

      val (bots, allowed) = initialBatch.partition(e => e.ip == botIp && e.url == botClickUrl)
      val expectedEvaluatedBots = bots.map(b => EvaluatedAdEventT(b, isBot = true))
      val expectedEvaluatedNormal = allowed.map(e => EvaluatedAdEventT(e, isBot = false))

      // WHEN
      eventsStream.addData(initialBatch)
      processDataWithLock7(query)

      // THEN
      val resultBatch1DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection1 = resultBatch1DS.collect()

      assert(resultEventsCollection1.length == 21)
      expectedEvaluatedBots.foreach(b => {
        assert(resultEventsCollection1.contains(b))
      })
      expectedEvaluatedNormal.foreach(e => {
        assert(resultEventsCollection1.contains(e))
      })

      // Clean-up the context
      cleanContext()
    }

    "detect bots within the 2nd window" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 11 events with 10 bot clicks
      // * bot clicks should be evaluated as normal since the number is 10
      // The 2nd:
      // * 11 events with 10 bot clicks
      // * bot clicks should be evaluated as bot since the total number of bots is 20 and interval is 3 seconds

      // GIVEN
      val eventsStream = MemoryStream[AdEventWithID]
      val eventsTypedStream: Dataset[AdEventWithID] = eventsStream.toDS()
      val evaluatedEventsStream = EventsProcessorStructured.evaluateAdEvents(eventsTypedStream)

      query = evaluatedEventsStream.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode(OutputMode.Update())
        .start()

      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      // First part of bots for the 1st window
      val botTime1 = System.currentTimeMillis() - Duration(5, TimeUnit.SECONDS).toMillis
      // Second part of bots 3 seconds later for the 2nd window
      val botTime2 = botTime1 + Duration(3, TimeUnit.SECONDS).toMillis

      // 10 bots in the 1st window
      val batch1 = EventsGenerator.generateAdEvents(
        eventsNumber = 11,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).map(e => toAdEventWithID(e))

      // 10 bots in the 2nd window
      val batch2 = EventsGenerator.generateAdEvents(
        eventsNumber = 11,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).map(e => toAdEventWithID(e))

      val (bots1, allowed1) = batch1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val expectedEvaluatedNormal_1 = allowed1.map(e => EvaluatedAdEventT(e, isBot = false))
      val expectedEvaluatedBots_1 = bots1.map(b => EvaluatedAdEventT(b, isBot = false))

      val (bots2, allowed2) = batch2.partition(e => e.ip == botIp && e.url == botClickUrl)
      val expectedEvaluatedBots_2 = bots2.map(b => EvaluatedAdEventT(b, isBot = true))
      val expectedEvaluatedNormal_2 = allowed2.map(e => EvaluatedAdEventT(e, isBot = false))

      // WHEN - process 1st batch
      eventsStream.addData(batch1)
      processDataWithLock15(query)

      // THEN
      val resultBatch1DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection1 = resultBatch1DS.collect()

      assert(resultEventsCollection1.length == 11)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection1.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection1.contains(e))
      })


      // WHEN  - process 2nd batch
      eventsStream.addData(batch2)
      processDataWithLock15(query)

      // THEN
      val resultBatch2DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection2 = resultBatch2DS.collect()

      assert(resultEventsCollection2.length == 22)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedBots_2.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })
      expectedEvaluatedNormal_2.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })

      // Clean-up the context
      cleanContext()
    }

    "should not detect bots within the 2nd window since the interval is more than 10 seconds" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 11 events with 10 bot clicks
      // * bot clicks should be evaluated as normal since the number is 10
      // The 2nd:
      // * 11 events with 10 bot clicks but the interval is 15 seconds
      // * bot clicks should be evaluated as allowed

      // GIVEN
      val eventsStream = MemoryStream[AdEventWithID]
      val eventsTypedStream: Dataset[AdEventWithID] = eventsStream.toDS()
      val evaluatedEventsStream = EventsProcessorStructured.evaluateAdEvents(eventsTypedStream)

      query = evaluatedEventsStream.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode(OutputMode.Update())
        .start()

      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      // First part of bots for the 1st window
      val botTime1 = System.currentTimeMillis() - Duration(10, TimeUnit.SECONDS).toMillis
      // Second part of bots 3 seconds later for the 2nd window
      val botTime2 = botTime1 + Duration(15, TimeUnit.SECONDS).toMillis

      // 10 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 11,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).map(e => toAdEventWithID(e))

      // 10 bots in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 11,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).map(e => toAdEventWithID(e))

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val expectedEvaluatedNormal_1 = allowed1.map(e => EvaluatedAdEventT(e, isBot = false))
      // First part of clicks should be treated as allowed since the number is 10
      val expectedEvaluatedBots_1 = bots1.map(b => EvaluatedAdEventT(b, isBot = false))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be also treated as allowed since the interval is 15 seconds
      val expectedEvaluatedBots_2 = bots2.map(b => EvaluatedAdEventT(b, isBot = false))
      val expectedEvaluatedNormal_2 = allowed2.map(e => EvaluatedAdEventT(e, isBot = false))

      // WHEN - process 1st batch
      eventsStream.addData(events1)
      processDataWithLock7(query)

      // THEN
      val resultBatch1DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection1 = resultBatch1DS.collect()

      assert(resultEventsCollection1.length == 11)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection1.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection1.contains(e))
      })

      // WHEN  - process 2nd batch
      eventsStream.addData(events2)
      processDataWithLock7(query)

      // THEN
      val resultBatch2DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection2 = resultBatch2DS.collect()

      assert(resultEventsCollection2.length == 22)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedBots_2.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })
      expectedEvaluatedNormal_2.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })

      // Clean-up the context
      cleanContext()
    }

    "detect late bots within the 2nd window" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 11 events with 10 old bot clicks (9 bots are 7 min late and 1 bot is 7 min - 5 sec late)
      // * bot clicks should be evaluated as normal since the number is 10
      // The 2nd:
      // * 11 events with 10 old bot clicks (all are 7 min - 5 sec late)
      // * bot clicks should be evaluated as bot since the total number of bots is 20 and belong to the same 10 sec interval

      // GIVEN
      val eventsStream = MemoryStream[AdEventWithID]
      val eventsTypedStream: Dataset[AdEventWithID] = eventsStream.toDS()
      val evaluatedEventsStream = EventsProcessorStructured.evaluateAdEvents(eventsTypedStream)

      query = evaluatedEventsStream.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode(OutputMode.Update())
        .start()

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
      ).map(toAdEventWithID).head ::
        EventsGenerator.generateAdEvents(
          eventsNumber = 10,
          botsNumberOpt = Some(9),
          botIpOpt = Some(botIp),
          botUrlOpt = Some(botClickUrl),
          botTimeOpt = Some(botTime1)
        ).map(toAdEventWithID).toList

      // 10 bots in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 11,
        botsNumberOpt = Some(10),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).map(toAdEventWithID)

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val expectedEvaluatedNormal_1 = allowed1.map(e => EvaluatedAdEventT(e, isBot = false))
      val expectedEvaluatedBots_1 = bots1.map(b => EvaluatedAdEventT(b, isBot = false))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      val expectedEvaluatedBots_2 = bots2.map(b => EvaluatedAdEventT(b, isBot = true))
      val expectedEvaluatedNormal_2 = allowed2.map(e => EvaluatedAdEventT(e, isBot = false))

      // WHEN - process 1st batch
      eventsStream.addData(events1)
      processDataWithLock15(query)

      // THEN
      val resultBatch1DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection1 = resultBatch1DS.collect()

      assert(resultEventsCollection1.length == 11)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection1.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection1.contains(e))
      })

      // WHEN  - process 2nd batch
      eventsStream.addData(events2)
      processDataWithLock15(query)

      // THEN
      val resultBatch2DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection2 = resultBatch2DS.collect()

      assert(resultEventsCollection2.length == 22)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedBots_2.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })
      expectedEvaluatedNormal_2.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })

      // Clean-up the context
      cleanContext()
    }

    "ignore clicks older 10 minutes" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 1 event with current event time
      // The 2nd:
      // * 20 bot events 12 minutes late
      // * bot clicks should be ignored due to latency

      // GIVEN
      val eventsStream = MemoryStream[AdEventWithID]
      val eventsTypedStream: Dataset[AdEventWithID] = eventsStream.toDS()
      val evaluatedEventsStream = EventsProcessorStructured.evaluateAdEvents(eventsTypedStream)

      query = evaluatedEventsStream.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode(OutputMode.Update())
        .start()

      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val currentClick = System.currentTimeMillis()
      val oldBotsTime = currentClick - Duration(12, TimeUnit.MINUTES).toMillis

      // 10 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 1,
        botsNumberOpt = Some(1),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(currentClick)
      ).map(toAdEventWithID)

      val events2 = EventsGenerator.generateAdEvents(
      eventsNumber = 20,
      botsNumberOpt = Some(20),
      botIpOpt = Some(botIp),
      botUrlOpt = Some(botClickUrl),
      botTimeOpt = Some(oldBotsTime)
      ).map(toAdEventWithID)

      val expectedEvaluatedNormal_1 = events1.map(e => EvaluatedAdEventT(e, isBot = false))

      // WHEN
      eventsStream.addData(events1)
      processDataWithLock7(query)

      // THEN
      val resultBatch1DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection1 = resultBatch1DS.collect()

      assert(resultEventsCollection1.length == 1)
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection1.contains(e))
      })

      // WHEN  - process 2nd batch
      eventsStream.addData(events2)
      processDataWithLock7(query)

      // THEN
      val resultBatch2DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection2 = resultBatch2DS.collect()

      assert(resultEventsCollection2.length == 1)
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })

      // Clean-up the context
      cleanContext()
    }

    "whitelist a host after 10 minutes a bot detection condition not matches" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 20 events with 20 bot clicks
      // * bot clicks should be evaluated as bot
      // The 2nd:
      // * 2 events with 1 bot click (11 minutes after ones in the 1st window)
      // * bot host should be whitelisted

      // GIVEN
      val eventsStream = MemoryStream[AdEventWithID]
      val eventsTypedStream: Dataset[AdEventWithID] = eventsStream.toDS()
      val evaluatedEventsStream = EventsProcessorStructured.evaluateAdEvents(eventsTypedStream)

      query = evaluatedEventsStream.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode(OutputMode.Update())
        .start()

      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val currentTime = System.currentTimeMillis()
      // First part of bots for the 1st window
      val botTime1 = currentTime - Duration(9, TimeUnit.MINUTES).toMillis
      // Second part of bots for the 2nd window
      val botTime2 = botTime1 + Duration(11, TimeUnit.MINUTES).toMillis

      // 25 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 20,
        botsNumberOpt = Some(20),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).map(toAdEventWithID)

      // 1 bot in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 2,
        botsNumberOpt = Some(1),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).map(toAdEventWithID)

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val expectedEvaluatedNormal_1 = allowed1.map(e => EvaluatedAdEventT(e, isBot = false))
      // First part of bot clicks should be evaluated as bots
      val expectedEvaluatedBots_1 = bots1.map(b => EvaluatedAdEventT(b, isBot = true))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be evaluated as allowed since bot detection condition not matches more than 10 minutes
      val expectedEvaluatedBots_2 = bots2.map(b => EvaluatedAdEventT(b, isBot = false))
      val expectedEvaluatedNormal_2 = allowed2.map(e => EvaluatedAdEventT(e, isBot = false))

      // WHEN - process 1st batch
      eventsStream.addData(events1)
      processDataWithLock7(query)

      // THEN
      val resultBatch1DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection1 = resultBatch1DS.collect()

      assert(resultEventsCollection1.length == 20)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection1.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection1.contains(e))
      })

      // WHEN  - process 2nd batch
      eventsStream.addData(events2)
      processDataWithLock7(query)

      // THEN
      val resultBatch2DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection2 = resultBatch2DS.collect()

      assert(resultEventsCollection2.length == 22)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedBots_2.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })
      expectedEvaluatedNormal_2.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })

      // Clean-up the context
      cleanContext()
    }

    "not whitelist a host after 9 minutes a bot detection condition not matches" in {
      // Scenario
      // Given 2 subsequent windows
      // The 1st:
      // * 20 events with 20 bot clicks
      // * bot clicks should be evaluated as bot
      // The 2nd:
      // * 1 events with 1 bot click (9 minutes after ones in the 1st window)
      // * bot host should be whitelisted

      // GIVEN
      val eventsStream = MemoryStream[AdEventWithID]
      val eventsTypedStream: Dataset[AdEventWithID] = eventsStream.toDS()
      val evaluatedEventsStream = EventsProcessorStructured.evaluateAdEvents(eventsTypedStream)

      query = evaluatedEventsStream.writeStream
        .format("memory")
        .queryName(queryName)
        .outputMode(OutputMode.Update())
        .start()

      val botIp = "1.1.1.1"
      val botClickUrl = "http://ads.com"
      val currentTime = System.currentTimeMillis()
      // First part of bots for the 1st window
      val botTime1 = currentTime - Duration(9, TimeUnit.MINUTES).toMillis
      // Second part of bots for the 2nd window
      val botTime2 = currentTime

      // 25 bots in the 1st window
      val events1 = EventsGenerator.generateAdEvents(
        eventsNumber = 20,
        botsNumberOpt = Some(20),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime1)
      ).map(toAdEventWithID)

      // 1 bot in the 2nd window
      val events2 = EventsGenerator.generateAdEvents(
        eventsNumber = 1,
        botsNumberOpt = Some(1),
        botIpOpt = Some(botIp),
        botUrlOpt = Some(botClickUrl),
        botTimeOpt = Some(botTime2)
      ).map(toAdEventWithID)

      val (bots1, allowed1) = events1.partition(e => e.ip == botIp && e.url == botClickUrl)
      val expectedEvaluatedNormal_1 = allowed1.map(e => EvaluatedAdEventT(e, isBot = false))
      // First part of bot clicks should be evaluated as bots
      val expectedEvaluatedBots_1 = bots1.map(b => EvaluatedAdEventT(b, isBot = true))

      val (bots2, allowed2) = events2.partition(e => e.ip == botIp && e.url == botClickUrl)
      // Second part of bot clicks should be evaluated as allowed since bot detection condition not matches more than 10 minutes
      val expectedEvaluatedBots_2 = bots2.map(b => EvaluatedAdEventT(b, isBot = true))
      val expectedEvaluatedNormal_2 = allowed2.map(e => EvaluatedAdEventT(e, isBot = false))

      // WHEN - process 1st batch
      eventsStream.addData(events1)
      processDataWithLock7(query)

      // THEN
      val resultBatch1DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection1 = resultBatch1DS.collect()

      assert(resultEventsCollection1.length == 20)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection1.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection1.contains(e))
      })

      // WHEN  - process 2nd batch
      eventsStream.addData(events2)
      processDataWithLock7(query)

      // THEN
      val resultBatch2DS = spark.table(queryName).as[EvaluatedAdEventT]
      val resultEventsCollection2 = resultBatch2DS.collect()

      assert(resultEventsCollection2.length == 21)
      expectedEvaluatedBots_1.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedBots_2.foreach(b => {
        assert(resultEventsCollection2.contains(b))
      })
      expectedEvaluatedNormal_1.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })
      expectedEvaluatedNormal_2.foreach(e => {
        assert(resultEventsCollection2.contains(e))
      })

      // Clean-up the context
      cleanContext()
    }

  }

  private def toAdEventWithID(e: AdEvent) = {
    AdEventWithID(UUID.randomUUID().toString, e.`type`, e.ip, e.event_time, e.url)
  }

  def processDataWithLock7(query: StreamingQuery): Unit = {
    query.processAllAvailable()
    while (query.status.message != "Waiting for data to arrive") {
      log.warn(s"Waiting for the query to finish processing, current status is ${query.status.message}")
      Thread.sleep(1)
    }
    log.warn("Locking the thread for another 7 seconds for state operations cleanup")
    Thread.sleep(7000)
  }

  def processDataWithLock15(query: StreamingQuery): Unit = {
    query.processAllAvailable()
    while (query.status.message != "Waiting for data to arrive") {
      log.warn(s"Waiting for the query to finish processing, current status is ${query.status.message}")
      Thread.sleep(1)
    }
    log.warn("Locking the thread for another 15 seconds for state operations cleanup")
    Thread.sleep(15000)
  }

  def cleanContext(): Unit = {
    if (query.isActive) {
      while (query.isActive) {
        query.stop()
        query.awaitTermination(1000)
      }
    }
    spark.sql(s"DROP TABLE IF EXISTS $queryName")
  }

}
