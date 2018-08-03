package com.guanshi.cloudhotel.etl

import com.guanshi.cloudhotel.util.SparkUtils

// 每月导入新增数据
class LoadData(master: String) {
  val spark = SparkUtils(master, "load data").spark

  import spark.implicits._
  import spark.sql

  def loadData(tableName: String, month: String): Unit = {
    val df = SparkUtils.getFromPostgres(spark, s"wsc.$tableName")
    df.createOrReplaceTempView(s"tv_$tableName")
    sql(
      s"""
         |insert into wsc.$tableName partition(month='$month')
         |select * from tv_$tableName
      """.stripMargin)
    //sql("select * from wsc.$tableName").show()
  }
}

object LoadData {
  def main(args: Array[String]): Unit = {
    val Array(master, month) = args
    val loadData = new LoadData(master)
    //loadData.loadData("tb_bookedroom", month)
    //loadData.loadData("tb_customer",month)
  }
}
