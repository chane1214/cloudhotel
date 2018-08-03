package com.guanshi.cloudhotel.etl

import java.util.Properties

import org.apache.spark.sql.SparkSession

object DataFromPostToHive {
  val spark = SparkSession.builder().master("local[*]").appName("DataFromPostToHive").enableHiveSupport().getOrCreate()
  val url = "jdbc:postgresql://localhost:5432/test"
  val conf = new Properties()
  conf.setProperty("user", "postgres")
  conf.setProperty("password", "1234")
  conf.put("driver", "org.postgresql.Driver")

  import spark.sql

  def readBookedroomFromPost(): Unit = {
    val df = spark.read.jdbc(url, "wsc.tb_bookedroom", conf)
    df.createOrReplaceTempView("tb_bookedroom")
    sql("drop table if exists wsc.bookedroom")
    sql(
      """
        |create table if not exists wsc.bookedroom as
        |select * from tb_bookedroom
      """.stripMargin
    )
    sql("select * from wsc.bookedroom").show()
  }

  def readBookingFromPost(): Unit = {
    val df = spark.read.jdbc(url, "wsc.tb_booking", conf)
    df.createOrReplaceTempView("tb_booking")
    sql("use wsc")
    sql("drop table if exists booking")
    sql(
      """
        |create  table if not exists booking as
        |select * from tb_booking
      """.stripMargin)
    //sql("select * from booking").take(10).foreach(println)
  }

  def readCustomerFromPost(): Unit = {
    val df = spark.read.jdbc(url, "wsc.tb_customer", conf)
    df.createOrReplaceTempView("tb_customer")
    sql("use wsc")
    sql("drop table if exists customer")
    sql(
      """
        |create  table if not exists customer as
        |select * from tb_customer
      """.stripMargin)
    //sql("select * from customer").show()
  }

  def readCheckInFromPost(): Unit = {
    val df = spark.read.jdbc(url, "wsc.tb_check_in", conf)
    df.createOrReplaceTempView("tb_check_in")
    sql("use wsc")
    sql("drop table if exists checkIn")
    sql(
      """
        |create  table if not exists checkIn as
        |select * from tb_check_in
      """.stripMargin)
    //sql("select * from checkIn").show()
  }

  def readCheckInRoomFromPost(): Unit = {
    val df = spark.read.jdbc(url, "wsc.tb_checkin_room", conf)
    df.createOrReplaceTempView("tb_checkin_room")
    sql("use wsc")
    sql("drop table if exists checkInRoom")
    sql(
      """
        |create  table if not exists checkInRoom as
        |select * from tb_checkin_room
      """.stripMargin)
    //sql("select * from checkInRoom").show()
  }

  def readRoomInfoFromPost(): Unit = {
    val df = spark.read.jdbc(url, "wsc.tb_roominfo", conf)
    df.createOrReplaceTempView("tb_roominfo")
    sql("drop table if exists wsc.roomInfo")
    sql(
      """
        |create table if not exists wsc.roomInfo as
        |select * from tb_roominfo
      """.stripMargin
    )
    sql("select * from wsc.roomInfo").show()
  }

  def main(args: Array[String]): Unit = {
    //readBookedroomFromPost()
    //readBookingFromPost()
    //readCustomerFromPost()
    //readCheckInFromPost()
    //readCheckInRoomFromPost()
    //readRoomInfoFromPost()
  }
}
