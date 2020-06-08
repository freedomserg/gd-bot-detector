package net

import org.apache.spark.streaming.{Duration, Minutes, Seconds}

package object learningclub {

  def filterOutOldClicks(clicks: List[Long]): List[Long] = {
    // keep only 10 minutes old events over the last seen time
    val lastSeenTime = clicks.max
    val detectionTimeLimit = (Duration(lastSeenTime) - Duration(Minutes(10).milliseconds)).milliseconds
    val filteredClicks = clicks.filter(_ >= detectionTimeLimit)
    filteredClicks
  }

  def detectBotForAllWindows(clicks: List[Long]): BotDetection = {
    val sortedClicks = clicks.sorted
    sortedClicks
      .indices
      .foldRight(BotDetection())((index, detection) => {
        if (detection.isBot) detection
        else {
          val latestClicks = sortedClicks.slice(index, sortedClicks.size)
          val filteredTenSecClicks = filterTenSecondsClicks(latestClicks)
          val isBot = filteredTenSecClicks.size >= 20
          if (isBot) BotDetection(isBot, Some(filteredTenSecClicks.max))
          else detection
        }
      })
  }

  def filterTenSecondsClicks(clicks: List[Long]): List[Long] = {
    val firstClickTime = clicks.min
    val detectionTimeLimit = (Duration(firstClickTime) + Duration(Seconds(10).milliseconds)).milliseconds
    val filteredClicks = clicks.filter(_ <= detectionTimeLimit)
    filteredClicks
  }

  def calculateNewHostStateForAllWindows(clicks: List[Long]): HostState = calculateNewHostState(clicks, detectBotForAllWindows)

  def calculateNewHostStateForSingleWindow(clicks: List[Long]): HostState = calculateNewHostState(clicks, detectBotForSingleWindow)

  def calculateNewHostState(clicks: List[Long], calcStateFn: List[Long] => BotDetection): HostState = {
    val tenMinClicks = filterOutOldClicks(clicks)
    val botDetection = calcStateFn(tenMinClicks)
    HostState(tenMinClicks, botDetection)
  }

  def detectBotForSingleWindow(clicks: List[Long]): BotDetection = {
    val isBot = clicks.size >= 20
    if (isBot) BotDetection(isBot, Some(clicks.max))
    else BotDetection()
  }

}
