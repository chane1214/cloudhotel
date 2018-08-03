package com.guanshi.cloudhotel.monitor

import java.sql.Date
import java.text.SimpleDateFormat

import com.guanshi.cloudhotel.entity.Entitys.{CommentAnalyze, CommentInfo}
import com.guanshi.cloudhotel.util.SparkUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream

class BadCommentMonitor(val ssc: StreamingContext) {
  // 从kafka中获取流数据
  def getCommentDataStream(): DStream[CommentInfo] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092,slaver1:9092,slaver2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "comment_monitor_spark_streaming",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("comment_info")
    val commentDstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    commentDstream.map(record => {
      val infos = record.value().split('|')
      CommentInfo(infos(0), infos(1).toInt, infos(2), infos(3).toInt, infos(4).toInt, infos(5).toInt, infos(6).toInt, infos(7).toInt)
    })
  }

  //分析差评，保存到mysql中
  def badCommentMonitor(): Unit = {
    val commentStream = getCommentDataStream()
    commentStream.foreachRDD(rdd => {
      rdd.filter(x => x.sumScore <= 3).foreachPartition(comments => {
        val conn = SparkUtils.getMysqlConnection()
        val sql = "insert into bad_comment_monitor values(?,?,?,?,?,?,?,?,?,?)"
        val preStatment = conn.prepareStatement(sql)
        comments.foreach(x => {
          preStatment.setDate(1, new Date(System.currentTimeMillis()))
          preStatment.setString(2, x.platForm)
          preStatment.setInt(3, x.hotelId)
          preStatment.setString(4, x.comment)
          preStatment.setInt(5, x.sumScore)
          preStatment.setInt(6, x.cleanScore)
          preStatment.setInt(7, x.serviceScore)
          preStatment.setInt(8, x.sercurityScore)
          preStatment.setInt(9, x.styleScore)
          preStatment.setString(10, if (x.sumScore == 3) "中等风险" else "高风险")
          preStatment.addBatch()
        })
        preStatment.executeBatch()
      })
    })
    /*CREATE TABLE bad_comment_monitor(
      monit_date DATE,
      plat_form VARCHAR(200),
      hotel_id INTEGER,
      c_comment VARCHAR(255),
      sum_score INTEGER,
      clean_score INTEGER,
      service_score INTEGER,
      sercurity_score INTEGER,
      style_score INTEGER,
      importent_level VARCHAR(30)
    )*/
  }

  //实时统计当日，每个酒店的，累计评论次数，最高评分，最低评分，各个小分数（服务，干净，风格，安全）的平均分
  //更新到数据库中
  def commentAnalyze(): Unit = {
    val commentStream = getCommentDataStream()
    ssc.checkpoint("checkpoint")
    val rdd = commentStream.map(x => {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      ((x.hotelId, format.format(new java.util.Date())), x)
    })
    val resultDStream = rdd.updateStateByKey((comments: Seq[CommentInfo], state: Option[CommentAnalyze]) => {
      if (comments != null && comments.size > 0) {
        var count = 0
        var maxScore = 0
        var minScore = 5
        var cleanSum = 0
        var serviceSum = 0
        var sercuritySum = 0
        var styleSum = 0
        comments.foreach(commentInfo => {
          count += 1
          maxScore = if (commentInfo.sumScore > maxScore) commentInfo.sumScore else maxScore
          minScore = if (commentInfo.sumScore < minScore) commentInfo.sumScore else minScore
          cleanSum += commentInfo.cleanScore
          serviceSum += commentInfo.serviceScore
          sercuritySum += commentInfo.sercurityScore
          styleSum += commentInfo.styleScore
        })
        val result = state match {
          case None => {
            CommentAnalyze(
              count, maxScore, minScore,
              1.0 * sercuritySum / count,
              1.0 * cleanSum / count,
              1.0 * serviceSum / count,
              1.0 * styleSum / count
            )
          }
          case Some(x) => {
            CommentAnalyze(
              (x.count + count),
              if (x.maxScore > maxScore) x.maxScore else maxScore,
              if (x.minScore < minScore) x.minScore else minScore,
              (x.cleanAvg * x.count + cleanSum) / (x.count + count),
              (x.serviceAvg * x.count + serviceSum) / (x.count + count),
              (x.sercurityAvg * x.count + sercuritySum) / (x.count + count),
              (x.styleAvg * x.count + styleSum) / (x.count + count)
            )
          }
        }
        Some(result)
      } else {
        None
      }
    })
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(infos => {
        val connection = SparkUtils.getMysqlConnection()
        val sql = "insert into comment_analyze values(?,?,?,?,?,?,?,?,?)"
        val preStatement = connection.prepareStatement(sql)
        infos.foreach(x => {
          val date = x._1._2
          val hotelId = x._1._1
          val result = x._2
          //println(result)
          preStatement.setString(1, date)
          preStatement.setInt(2, hotelId)
          preStatement.setInt(3, result.count)
          preStatement.setInt(4, result.maxScore)
          preStatement.setInt(5, result.minScore)
          preStatement.setDouble(6, result.cleanAvg)
          preStatement.setDouble(7, result.serviceAvg)
          preStatement.setDouble(8, result.sercurityAvg)
          preStatement.setDouble(9, result.styleAvg)
          preStatement.addBatch()
        })
        preStatement.executeBatch()
      })
    })
    /*CREATE TABLE comment_analyze(
      date DATE,
      hotel_id INTEGER,
      count INTEGER,
      max_score INTEGER,
      min_score INTEGER,
      clean_avg DOUBLE,
      service_avg DOUBLE,
      sercurity_avg DOUBLE,
      style_avg DOUBLE
    )*/
  }
}

object BadCommentMonitor {
  def apply(ssc: StreamingContext) = new BadCommentMonitor(ssc)
}

object BadCommentMonitorTest {
  def main(args: Array[String]): Unit = {
    val ssc = SparkUtils("local[*]", "badcomment monitor").ssc
    val badCommentMonitor = BadCommentMonitor(ssc)
    //badCommentMonitor.badCommentMonitor()
    badCommentMonitor.commentAnalyze()
    ssc.start()
    ssc.awaitTermination()
  }
}
