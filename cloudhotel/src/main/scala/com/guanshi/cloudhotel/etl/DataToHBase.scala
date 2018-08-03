/*
package com.guanshi.cloudhotel.etl

import java.sql.Timestamp

import com.guanshi.cloudhotel.entity.Entitys.BookedRoom
import com.guanshi.cloudhotel.util.SparkUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job

class DataToHBase(master: String) {
  val sc = SparkUtils(master, "data to hbase").sc
  val spark = SparkUtils(master, "load data").spark
  val hbaseConf = HBaseConfiguration.create()

  import spark.sql

  def writeToHbase(): Unit = {
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "spark_hotel")
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[Text]])

    val df = sql("select * from wsc.bookedroom")
      .map(row => BookedRoom(
        row.getAs[Int]("bookedroom_id"),
        row.getAs[String]("livetype"),
        row.getAs[java.math.BigDecimal]("room_money"),
        row.getAs[Timestamp]("booktime"),
        row.getAs[Int]("bookvalid_flag"),
        row.getAs[java.math.BigDecimal]("paymoney"),
        row.getAs[String]("disable_reason"),
        row.getAs[Int]("booking_id"),
        row.getAs[Int]("roominfo_id"),
        row.getAs[Int]("disable_staff_id"),
        row.getAs[Timestamp]("disable_time"),
        row.getAs[Int]("checkin_room_id"),
      ))

    df.map(x => {
      val rowKey = x.bookedroom_id
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn("i".getBytes(), "livetype".getBytes(), Bytes.toBytes(x.livetype))
      put.addColumn("i".getBytes(), "room_money".getBytes(), Bytes.toBytes(x.room_money))
      put.addColumn("i".getBytes(), "booktime".getBytes(), Bytes.toBytes(x.booktime.toString))
      put.addColumn("i".getBytes(), "bookvalid_flag".getBytes(), Bytes.toBytes(x.bookvalid_flag))
      put.addColumn("i".getBytes(), "paymoney".getBytes(), Bytes.toBytes(x.paymoney))
      put.addColumn("i".getBytes(), "disable_reason".getBytes(), Bytes.toBytes(x.disable_reason))
      put.addColumn("i".getBytes(), "booking_id".getBytes(), Bytes.toBytes(x.booking_id))
      put.addColumn("i".getBytes(), "roominfo_id".getBytes(), Bytes.toBytes(x.roominfo_id))
      put.addColumn("i".getBytes(), "disable_staff_id".getBytes(), Bytes.toBytes(x.disable_staff_id))
      put.addColumn("i".getBytes(), "disable_time".getBytes(), Bytes.toBytes(x.disable_time.toString))
      put.addColumn("i".getBytes(), "checkin_room_id".getBytes(), Bytes.toBytes(x.checkin_room_id))
      (rowKey, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

object DataToHBase {
  def main(args: Array[String]): Unit = {
    val Array(master) = args
    val dataToHBase = new DataToHBase(master)
    dataToHBase.writeToHbase()
  }
}
*/
