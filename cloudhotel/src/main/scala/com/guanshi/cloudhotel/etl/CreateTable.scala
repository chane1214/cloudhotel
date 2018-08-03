package com.guanshi.cloudhotel.etl

import com.guanshi.cloudhotel.util.SparkUtils

// 创建按月分区表
class CreateTable(master: String) {
  val spark = SparkUtils(master, "create table").spark

  import spark.implicits._
  import spark.sql

  def createTableBookedroom(): Unit = {
    sql(
      """
        |create table if not exists wsc.tb_bookedroom(
        | bookedroom_id int,
        | livetype string,
        | room_money double,
        | booktime date,
        | bookvalid_flag int,
        | paymoney double,
        | disable_reason string,
        | booking_id int,
        | roominfo_id int,
        | disable_staff_id int,
        | disable_time date,
        | checkin_room_id int
        |)
        |partitioned by (month string)
      """.stripMargin)
  }

  def createTableCustomer(): Unit = {
    sql(
      """
        |create table if not exists wsc.tb_customer(
        | customer_id int,
        | customer_name string,
        | sex string,
        | certificate_type string,
        | certificate_no string,
        | address string,
        | phoneno string,
        | note string,
        | taobao_account string,
        | hyunit string,
        |)
        |partitioned by (month string)
      """.stripMargin)
  }
}

object CreateTable {
  def main(args: Array[String]): Unit = {
    val Array(master) = args
    val createTable = new CreateTable(master)
    //createTable.createTableBookedroom()
    //createTable.createTableCustomer()
  }
}
