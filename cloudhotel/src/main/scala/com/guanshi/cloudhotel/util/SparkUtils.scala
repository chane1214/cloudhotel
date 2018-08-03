package com.guanshi.cloudhotel.util

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

class SparkUtils(val master: String, val appName: String) {
  val sparkConf = if (master != null && master != "") {
    new SparkConf().setMaster(master).setAppName(appName)
  } else {
    new SparkConf().setAppName(appName)
  }

  def sc = new SparkContext(sparkConf)

  def spark = if (master != null && master != "") {
    SparkSession.builder().appName(appName).master(master).enableHiveSupport().getOrCreate()
  } else {
    SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
  }

  var duration = Seconds(3)

  def ssc = new StreamingContext(sparkConf, duration)

  def beginStreaming(): Unit = {
    ssc.start()
    ssc.awaitTermination()
  }
}

object postgresConf {
  val url = "jdbc:postgresql://localhost:5432/test"
  val properties = new Properties()
  properties.setProperty("user", "postgres")
  properties.setProperty("password", "1234")
  properties.put("driver", "org.postgresql.Driver")
}

object mysqlConf {
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/test"
  val user = "root"
  val password = "1234"
}

object SparkUtils {
  def apply(master: String, appName: String): SparkUtils = new SparkUtils(master, appName)

  def newSC(sparkConf: SparkConf) = {
    new SparkContext(sparkConf)
  }

  def newSSC(sparkConf: SparkConf, duration: Duration) = {
    new StreamingContext(sparkConf, duration)
  }

  def getFromPostgres(spark: SparkSession, tableName: String): DataFrame = {
    spark.read.jdbc(postgresConf.url, tableName, postgresConf.properties)
  }

  def getFromPostgres(spark: SparkSession, tableName: String, columnName: String, lowerBound: Long, upperBound: Long, numPartitions: Int): DataFrame = {
    spark.read.jdbc(postgresConf.url, tableName, columnName, lowerBound, upperBound, numPartitions, postgresConf.properties)
  }

  def getMysqlConnection(): Connection = {
    Class.forName(mysqlConf.driver)
    DriverManager.getConnection(mysqlConf.url, mysqlConf.user, mysqlConf.password)
  }
}
