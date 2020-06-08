package net.learningclub

import java.sql.Timestamp
import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class AdEvent(
                    `type`: String,
                    ip: String,
                    event_time: Long,
                    url: String
                  ) {
  val id: String = UUID.randomUUID().toString
}

case class AdEventWithID(
                          id: String,
                          `type`: String,
                          ip: String,
                          event_time: Long,
                          url: String
                        )

case class AdEventTNoId(`type`: String,
                        ip: String,
                        event_time: String,
                        url: String
                       )

object AdEvent {
  implicit val adEventDecoder: Decoder[AdEvent] = deriveDecoder[AdEvent]
  implicit val adEventEncoder: Encoder[AdEvent] = deriveEncoder[AdEvent]
}

case class EvaluatedAdEventT(id: String,
                             `type`: String,
                             ip: String,
                             event_time: Timestamp,
                             url: String,
                             is_bot: Boolean)

object EvaluatedAdEventT {
  def apply(event: AdEventWithID, isBot: Boolean): EvaluatedAdEventT =
    EvaluatedAdEventT(event.id, event.`type`, event.ip, new Timestamp(event.event_time.toLong), event.url, isBot)
}

case class EvaluatedAdEvent(id: String,
                            `type`: String,
                            ip: String,
                            event_time: Long,
                            url: String,
                            is_bot: Boolean)

object EvaluatedAdEvent {
  def apply(event: AdEvent, isBot: Boolean): EvaluatedAdEvent =
    EvaluatedAdEvent(event.id, event.`type`, event.ip, event.event_time, event.url, isBot)
}

case class HostIp(ip: String)

case class UnparsableEvent(originalMessage: String, exception: Throwable)

case class HostState(recentClicks: List[Long], botDetection: BotDetection)

object HostState {
  implicit val hostStateDecoder: Decoder[HostState] = deriveDecoder[HostState]
  implicit val hostStateEncoder: Encoder[HostState] = deriveEncoder[HostState]
}

case class BotDetection(isBot: Boolean = false, detectionTime: Option[Long] = None)

object BotDetection {
  implicit val botDetectionDecoder: Decoder[BotDetection] = deriveDecoder[BotDetection]
  implicit val botDetectionEncoder: Encoder[BotDetection] = deriveEncoder[BotDetection]
}