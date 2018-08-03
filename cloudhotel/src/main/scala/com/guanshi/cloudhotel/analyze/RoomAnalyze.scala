package com.guanshi.cloudhotel.analyze

import java.util.Date

import com.guanshi.cloudhotel.DateUtil
import com.guanshi.cloudhotel.util.SparkUtils

class RoomAnalyze(master: String, date: Date) {
  val spark = SparkUtils(master, "room analyze").spark

  import spark.sql

  def roomRate(): Unit = {
    val firstDay = DateUtil.getLastDayOfMonth(date)
    val lastDay = DateUtil.getFirstDayOfMonth(date)
    val monthDays = DateUtil.getDaysOfMonth(date)
    sql("use wsc")
    //a:房间ID，酒店ID，价格，入住间夜数
    //b:房间ID，预定间夜数
    //c:房间ID，实际总金额
    // 酒店ID，房间ID，预定间夜数，入住间夜数，入住率，价格，总金额，实际总金额
    sql(
      s"""
         |select c.hotel_id hotel_id,
         |       a.roominfo_id roominfo_id,
         |       b.days1 bookDays,
         |       a.days2 trueDays,
         |       a.days2/$monthDays roomRate,
         |       a.roommoney roomMoney,
         |       a.roommoney*a.days2 bookSumfee,
         |       c.sumfee*a.days2 trueSumfee
         |from
         |   (select roominfo_id,
         |       min(room_money) roommoney,
         |       sum(datediff(true_chkout_time,checking_time)) days2
         |   from checkInRoom
         |   where checking_time between $firstDay and $lastDay
         |      and true_chkout_time between $firstDay and $lastDay
         |   group by roominfo_id)a,
         |
         |   (select roominfo_id,
         |       count(1) days1
         |   from bookedroom
         |   where booktime between $firstDay and $lastDay
         |   group by roominfo_id)b,
         |
         |   (select a.hotel_id hotel_id,
         |       b.roominfo_id,
         |       a.sum_fee sumfee
         |   from checkIn a,checkInRoom b
         |   where a.check_in_id=b.check_in_id)c
         |where a.roominfo_id=b.roominfo_id and a.roominfo_id=c.roominfo_id
      """.stripMargin)
      .createOrReplaceTempView("tv_roomRate")
    sql(
      """
        |create table if not exists roomRate as
        |select * from tv_roomRate
      """.stripMargin
    )
    //sql("select * from roomRate").show()
  }

  def roomTypeRate(): Unit = {
    val monthDays = DateUtil.getDaysOfMonth(date)
    sql("use wsc")
    // 酒店ID，房型，房间数，预定间夜数，入住间夜数，入住率，价格，总金额，实际总金额
    sql(
      s"""
         |select a.hotel_id,
         |       b.room_type_id,
         |       count(distinct b.roominfo_id),
         |       sum(a.bookDays),
         |       sum(a.trueDays),
         |       sum(a.trueDays)/$monthDays roomRate,
         |       min(a.roomMoney),
         |       sum(a.bookSumfee),
         |       sum(a.trueSumfee)
         |from roomRate a,roomInfo b
         |where a.roominfo_id=b.roominfo_id
         |group by a.hotel_id,b.room_type_id
      """.stripMargin)
      .createOrReplaceTempView("tv_roomTypeRate")
    sql(
      """
        |create table if not exists roomTypeRate as
        |select * from tv_roomTypeRate
      """.stripMargin
    )
    //sql("select * from roomTypeRate").show()
  }

  def hotelRate(): Unit = {
    val monthDays = DateUtil.getDaysOfMonth(date)
    sql("use wsc")
    // 酒店ID，房型数，房间数，预定间夜数，入住间夜数，入住率，价格，总金额，实际总金额
    sql(
      s"""
         |select a.hotel_id,
         |       count(distinct b.room_type_id),
         |       count(distinct b.roominfo_id),
         |       sum(a.bookDays),
         |       sum(a.trueDays),
         |       sum(a.trueDays)/$monthDays roomRate,
         |       min(a.roomMoney),
         |       sum(a.bookSumfee),
         |       sum(a.trueSumfee)
         |from roomRate a,roomInfo b
         |where a.roominfo_id=b.roominfo_id
         |group by a.hotel_id
      """.stripMargin)
      .createOrReplaceTempView("tv_roomTypeRate")
    sql(
      """
        |create table if not exists roomTypeRate as
        |select * from tv_roomTypeRate
      """.stripMargin
    )
    //sql("select * from roomTypeRate").show()
  }

  /*def test(): Unit = {
    val df = sql("select * from tb")
    val df1 = df.map(row => Cctest(row.getAs[Int]("Cctest_id"),...) )
    df1.createOrReplaceTempView("tv_1")
    sql("select * from tv_1 join tv_2 on a.id=b.id")
  }*/
}

object RoomAnalyze {
  def main(args: Array[String]): Unit = {
    val Array(master, date) = args
    val roomAnalyze = new RoomAnalyze(master, DateUtil.convert2Date(date, "yyyyMMdd"))
    //roomAnalyze.roomRate()
    //roomAnalyze.roomTypeRate()
    //roomAnalyze.hotelRate()
  }
}

