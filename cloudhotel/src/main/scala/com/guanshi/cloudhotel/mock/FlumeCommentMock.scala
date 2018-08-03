package com.guanshi.cloudhotel.mock

import java.nio.charset.Charset

import org.apache.flume.Event
import org.apache.flume.event.EventBuilder

import scala.util.Random

class FlumeCommentMock(val client: FlumeClient) {
  val platformType = List("携程", "艺龙", "去哪儿")
  val hotelIds = List(12, 1, 8, 4, 3, 9, 28, 29, 30, 33)
  val words = List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q")
  val random = new Random()

  def mockEvent(): Event = {
    val platform = platformType(random.nextInt(3))
    val hotelId = hotelIds(random.nextInt(10))
    val word = (for (wordPerLines <- 5 to 20) yield {
      (for (alphaPerWord <- 1 to 1 + random.nextInt(10)) yield words(random.nextInt(17))).mkString("")
    }).mkString("")
    val sumScore = 1 + random.nextInt(5)
    val cleanScore = 1 + random.nextInt(5)
    val serviceScore = 1 + random.nextInt(5)
    val sercurityScore = 1 + random.nextInt(5)
    val styleScore = 1 + random.nextInt(5)

    val finfos = s"$platform|$hotelId|$word|$sumScore|$cleanScore|$serviceScore|$sercurityScore|$styleScore"
    EventBuilder.withBody(finfos, Charset.forName("UTF-8"))
  }

  //随机产生一条评论信息并发送到flume中
  def sendRandomEvent(): Unit = {
    client.sendEvent(mockEvent())
  }
}

object FlumeCommock {
  def apply(client: FlumeClient) = new FlumeCommentMock(client)
}

object MockFlumeCommentData {
  def main(args: Array[String]): Unit = {
    val commentMock = FlumeCommock(FlumeClient("master", 33333))
    (1 until 100).foreach(x => {
      commentMock.sendRandomEvent()
    })
    commentMock.client.cleanUp()
  }
}