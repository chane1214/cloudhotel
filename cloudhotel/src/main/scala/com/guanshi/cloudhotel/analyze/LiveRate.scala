package com.guanshi.cloudhotel.analyze

import java.sql.Timestamp
import java.util.Date

import com.guanshi.cloudhotel.DateUtil
import com.guanshi.cloudhotel.entity.Entitys.LiveDays
import com.guanshi.cloudhotel.util.SparkUtils
import org.apache.spark.sql.Row

class LiveRate(master: String, appName: String, opDate: Date) {
  val spark = SparkUtils(master, appName).spark

  import spark.sql

  def test(): Unit = {
    val firstDayofMonth = DateUtil.getLastDayOfMonth(opDate)
    val lastDayOfMonth = DateUtil.getFirstDayOfMonth(opDate)
    val month = DateUtil.convert2String(opDate, "yyyyMM")
    val monthDays = DateUtil.getDaysOfMonth(opDate)
    val nowData = opDate
    //日期（月）酒店id 房间ID 预定间夜数 价格
    val bookDays = sql(
      """
        |select  a.hotel_id
        |        ,b.roominfo_id
        |        ,b.checking_time
        |        ,b.true_chkout_time
        |        ,a.sum_fee as room_money
        |from hoteldw.tb_check_in a
        |inner join hoteldw.tb_checkin_room b
        |on a.check_in_id=b.check_in_id
      """.stripMargin)
    val bookDays1 = bookDays.filter(row => {
      row.getAs[Timestamp]("checking_time") != null &&
        row.getAs[Timestamp]("true_chkout_time") != null &&
        !(row.getAs[Timestamp]("checking_time").after(firstDayofMonth) || row.getAs[Timestamp]("true_chkout_time").before(lastDayOfMonth))
    }).map(row => {
      val liveDays = LiveRate.getDays(row, nowData)
      LiveDays(row.getAs[Int]("hotel_id"), row.getAs[Int]("roominfo_id"), month, row.getAs[java.math.BigDecimal]("room_money"), liveDays, monthDays)
    })
    bookDays1.createOrReplaceTempView("live_days")
  }

  def checkInDays(): Unit = {
    sql(
      """
        |select month
        |       ,hotelId
        |       ,roomId
        |       ,sum(roomMoney) sumMoney
        |       ,sum(liveDays) sumLiveDays
        |       ,max(monthDays) allDays
        |       ,sum(liveDays)/max(monthDays) liveRate
        |from live_days
        |group by month
        |         ,hotelId
        |         ,roomId
      """.stripMargin).show
  }
}

object LiveRate {
  def apply(master: String, appName: String, opDate: Date): LiveRate = new LiveRate(master, appName, opDate: Date)

  def getDays(row: Row, mydate: Date): Int = {
    val inDate = Timestamp.valueOf(row.getAs[String]("checking_time"))
    val outDate = Timestamp.valueOf(row.getAs[String]("true_chkout_time"))
    //入住日期 和 退房日期如果都在当月的日期之内，则两个日期相减得到天数
    //入住日期小于当月第一天，退房日期在当月之内，则退房日期减去当月第一天得到入住天数
    //入住日期在当月之内，退房日期大于当月最后一天，则当月最后一天减去入住日期得到入住天数
    //入住日期和退房日期都在当月日期之外，当月的总天数就是入住天数
    val firstDay = DateUtil.setStartDay(DateUtil.getFirstDayOfMonth(mydate))
    val lastDay = DateUtil.setEndDay(DateUtil.getLastDayOfMonth(mydate))

    var caculLittleDay = if (firstDay.before(inDate)) inDate else firstDay
    var caculBiggerDay = if (lastDay.before(outDate)) DateUtil.addDays(lastDay, 1) else outDate

    //当日当日退算1天
    if (DateUtil.getDay(caculLittleDay) == DateUtil.getDay(caculBiggerDay)) {
      caculLittleDay = DateUtil.addDays(caculLittleDay, -1)
    }

    val days = DateUtil.diffDay(DateUtil.setStartDay(caculLittleDay), DateUtil.setStartDay(caculBiggerDay))
    println(s"$inDate---$outDate---$days")
    days
  }

  def main(args: Array[String]): Unit = {
    val liveRate = LiveRate("local[*]", "liveRate", DateUtil.convert2Date("20170303", "yyyyMMdd"))
    liveRate.test()
    liveRate.checkInDays()
  }
}
