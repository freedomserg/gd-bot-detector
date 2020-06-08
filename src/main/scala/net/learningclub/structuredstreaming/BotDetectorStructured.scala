package net.learningclub.structuredstreaming

import java.sql.Timestamp

import net.learningclub.structuredstreaming.EventsProcessorStructured.evaluateAdEvents
import net.learningclub.{AdEventTNoId, AdEventWithID, EvaluatedAdEventT}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


case class Window(start: Timestamp, end: Timestamp)

case class AdEventWithWindow(
                              id: String,
                              `type`: String,
                              ip: String,
                              event_time: Timestamp,
                              url: String,
                              window: Window
                            ) {
  def toAdEventWithID = AdEventWithID(id, `type`, ip, event_time.getTime, url)
}

object BotDetectorStructured extends App with Logging {

  val runLocal = true

  val sessionBuilder = SparkSession.builder()
    .master("local[4]") // 1 dedicated executor as a receiver allocated for input kafka stream for 1 source input, 3 - for data processing
    .appName("Add Events Processor")
    .config("spark.driver.memory", "2g")
    .config("spark.cassandra.connection.host", "localhost")
  val spark = if (runLocal) {
    sessionBuilder
      .config("spark.broadcast.compress", "false")
      .config("spark.shuffle.compress", "false")
      .config("spark.shuffle.spill.compress", "false")
      .getOrCreate()
  } else sessionBuilder.getOrCreate()

  val generateUuid = udf(() => java.util.UUID.randomUUID.toString)

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  //  spark.sparkContext.setLogLevel("WARN")


  import spark.implicits._

  val streamDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ad_events")
    //    .option("startingOffsets", "latest") // default
    .load()

  val eventSchema = new StructType()
    .add("type", StringType)
    .add("ip", StringType)
    .add("event_time", StringType)
    .add("url", StringType)

  val streamDS = streamDF
    .selectExpr("CAST(value AS STRING)")
    .where(from_json($"value", eventSchema).isNotNull)
    .select(from_json($"value", eventSchema).as[AdEventTNoId])
    .withColumn("id", generateUuid())
    .withColumn("event_time", $"event_time".cast(LongType))
    .as[AdEventWithID]

  evaluateAdEvents(streamDS)
    .writeStream
    .outputMode(OutputMode.Update())
    .foreachBatch { (batch: Dataset[EvaluatedAdEventT], _: Long) =>
      batch
        .write
        .cassandraFormat("ad_events", "events")
        .mode(SaveMode.Append)
        .save
    }

    .start()
    .awaitTermination()
}
