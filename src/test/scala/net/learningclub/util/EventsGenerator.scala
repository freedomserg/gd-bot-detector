package net.learningclub.util

import net.learningclub.AdEvent

import scala.util.Random

object EventsGenerator {

  private val bots = List("134.227.33.154", "145.169.2.69", "42.214.155.29")
  private val bot_urls = List("https://blog.griddynamics.com/in-stream-processing-service-blueprint/",
    "https://blog.griddynamics.com/how-in-stream-processing-works/",
    "https://blog.griddynamics.com/overview-of-in-stream-processing-solutions-on-the-market/")

  def generateRandomIpAddress(): String = {
    val r = Random
    r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256)
  }

  def generateRandomUrl(): String = {
    val size = 20
    val prefix = "http://"
    val randSuff = Random.alphanumeric.take(size).mkString
    s"$prefix$randSuff"
  }

  def generateAdEvents(
                        eventsNumber: Int,
                        botsNumberOpt: Option[Int] = None,
                        botIpOpt: Option[String] = None,
                        botUrlOpt: Option[String] = None,
                        botTimeOpt: Option[Long] = None): Seq[AdEvent] = {
    def generateRandomEvents(elemNum: Int): Seq[AdEvent] = {
      if (elemNum == 0) Nil
      else {
        for (_ <- 0 until elemNum) yield AdEvent(
          "click",
          generateRandomIpAddress(),
          System.currentTimeMillis(),
          generateRandomUrl()
        )
      }
    }

    def generateBotEvents(elemNum: Int): Seq[AdEvent] = {
      if (elemNum == 0) Nil
      else {
//        val botInd = Random.nextInt(bots.size)
        val botIp = botIpOpt.get
//        val botUrlInd = Random.nextInt(bot_urls.size)
        val botUrl = botUrlOpt.get
        val botTime = botTimeOpt.getOrElse(System.currentTimeMillis())
        for (_ <- 0 until elemNum) yield AdEvent("click", botIp, botTime, botUrl)
      }
    }

    def generateEventsSeq(elemNum: Int): Seq[AdEvent] = {
      def generateBotEventsNumber(elemNum: Int) = {
        val r = Random.nextInt(elemNum)
        if (elemNum >= 21 && r < 20) r + (20-r)
        else r
      }
      val botEventsNumber = if (botsNumberOpt.isDefined && botIpOpt.isDefined && botUrlOpt.isDefined)  botsNumberOpt.get else 0
      val randomEventsNumber = elemNum - botEventsNumber
      generateBotEvents(botEventsNumber) ++ generateRandomEvents(randomEventsNumber)
    }

    generateEventsSeq(eventsNumber)

  }
}
