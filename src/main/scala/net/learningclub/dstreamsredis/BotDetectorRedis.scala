package net.learningclub.dstreamsredis

import net.learningclub.{AdEvent, UnparsableEvent}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import io.circe.parser.decode

object BotDetectorRedis extends App with Logging {

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

  val checkpointDir = "checkpoint"

  val streamingContext = StreamingContext.getOrCreate(checkpointDir, () => {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint(checkpointDir)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "adevents-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("ad_events")

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val inputStream = kafkaStream.map(record => tryConversionToAdEvent(record.value()))
      .flatMap(_.right.toOption)
    EventsProcessorRedis.evaluateAdEvents(inputStream)
      .transform(dStreamRdd => {
        val sc = dStreamRdd.sparkContext
        sc.makeRDD(dStreamRdd.collect().flatten)
      })
      .saveToCassandra("events", "ad_events")
    ssc
  })


  streamingContext.start()
  streamingContext.awaitTermination()


  import scala.util.{Either, Left, Right}

  def tryConversionToAdEvent(eventLine: String): Either[UnparsableEvent, AdEvent] = {
    decode[AdEvent](eventLine).fold(
      failure => {
        log.warn(s"Parsing event record failed: $failure")
        Left(UnparsableEvent(eventLine, failure))
      }
      ,
      event => {
        Right(event)
      }
    )
  }


}
