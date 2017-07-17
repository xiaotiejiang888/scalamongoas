package com.mongo.spark

import java.io.{FileSystem => _, _}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HDFSFileService {
  def getOneDayStart_time():Long={
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    val str = a+""
    str.toLong
  }
  def getOneDayEnd_time():Long={
    var cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 999);
    val a = cal.getTime.getTime
    val str = a+""
    str.toLong
  }

  def getDt = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = dateFormat.format(now)
    dt
  }
  def getSparkSession(args: Array[String]): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://zmy3:zmy3@172.23.5.158/mpush.app?readPreference=primary")
    val conf = new SparkConf()
      .setMaster("spark://172.23.5.113:7077")
      .setAppName("MongoSparkConnectorTour")
      .set("spark.app.id", "MongoSparkConnectorTour")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)
      .set("aerospike.seedhost", "172.23.5.158")
      .set("aerospike.port",  "3000")
      .set("aerospike.namespace", "push")
    val spark = SparkSession.builder().config(conf).master("spark://172.23.5.113:7077").appName("scalamongoas").getOrCreate()
    spark
  }

  def main(args: Array[String]): Unit = {
    val session = getSparkSession(args)
    val sqlContext = session.sqlContext
    val dfApp = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.app?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner")))
    val dfTd = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.td?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner")))
    val dfTdAppWithOutId = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.td?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner"))).where("source='app'").drop("_id")
    val dfTdGameWithOutId = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.td?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner"))).where("source='game'").drop("_id")
    val dfPush = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.push?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner")))
    val dfActivity = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.activity?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner")))
    val sprkAsconf = new SparkConf().setMaster("spark://172.23.5.113:7077").setAppName("Aerospike Tests for Spark Dataset").set("aerospike.seedhost", "172.23.5.158").set("aerospike.port",  "3000").set("aerospike.namespace", "push")
    val dfDv = session.read.format("com.aerospike.spark.sql").option("aerospike.set", "dv").load
    val appApp = dfApp.join(dfTdAppWithOutId,dfApp("_id")===dfTdAppWithOutId("app"))
    var appGame = dfApp.join(dfTdGameWithOutId,dfApp("_id")===dfTdGameWithOutId("app"))
    dfApp.createOrReplaceTempView("dfApp")
    dfTd.createOrReplaceTempView("dfTd")
    var appMpush = sqlContext.sql("select * from dfApp  where _id not in (select app from dfTd)")
    val count = appApp.join(dfPush,appApp("_id")===dfPush("app")).select(dfPush("_id"),dfPush("ct")).where("ct> 1499616000000 and ct< 1499702400000").count()
    val fileName = "statistics_result_" + getDt + ".txt"
    val writer = new PrintWriter(new File(fileName))
    writer.write("app每日贡献push数:"+count)
    writer.close()
  }
}
