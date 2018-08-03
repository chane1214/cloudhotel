package com.guanshi.cloudhotel.analyze

import com.guanshi.cloudhotel.util.SparkUtils

class CustomerAnalyze(master: String) {
  val spark = SparkUtils(master, "room analyze").spark

  import spark.implicits._
  import spark.sql

  def customerRate(): Unit = {
    sql("use wsc")
    // 总用户数  总天数 人均天数 总消费金额 人均消费金额
    sql(
      """
        |select count(distinct a.customer_id),
        |       sum(datediff(d.true_chkout_time,d.checking_time)),
        |       sum(datediff(d.true_chkout_time,d.checking_time))/count(distinct a.customer_id),
        |       sum(c.sum_fee),
        |       sum(c.sum_fee)/count(distinct a.customer_id)
        |from customer a,booking b,checkIn c,checkInRoom d
        |where a.customer_id = b.customer_id
        |   and b.booking_id = c.booking_id
        |   and c.check_in_id = d.check_in_id
      """.stripMargin)
      .createOrReplaceTempView("tv_customerRate")
    sql(
      """
        |create table if not exists customerRate as
        |select * from tv_customerRate
      """.stripMargin
    )
    //sql("select * from customerRate").show()
  }

  def hotelcustomerRate(): Unit = {
    sql("use wsc")
    // 酒店ID，总用户数  总天数 人均天数 总消费金额 人均消费金额
    sql(
      """
        |select c.hotel_id,
        |       count(distinct a.customer_id),
        |       sum(datediff(d.true_chkout_time,d.checking_time)),
        |       sum(datediff(d.true_chkout_time,d.checking_time))/count(distinct a.customer_id),
        |       sum(c.sum_fee),
        |       sum(c.sum_fee)/count(distinct a.customer_id)
        |from customer a,booking b,checkIn c,checkInRoom d
        |where a.customer_id = b.customer_id
        |   and b.booking_id = c.booking_id
        |   and c.check_in_id = d.check_in_id
        |group by c.hotel_id
      """.stripMargin)
      .createOrReplaceTempView("tv_hotelcustomerRate")
    sql(
      """
        |create table if not exists hotelcustomerRate as
        |select * from tv_hotelcustomerRate
      """.stripMargin
    )
    //sql("select * from hotelcustomerRate").show()
  }
}

object CustomerAnalyze {
  def main(args: Array[String]): Unit = {
    val Array(master) = args
    val customerAnalyze = new CustomerAnalyze(master)
    //customerAnalyze.customerRate()
    //customerAnalyze.hotelcustomerRate()
  }
}
