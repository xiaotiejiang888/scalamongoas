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
    val dfPushOneDay = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.push?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner"))).where("ct>1499616000000 and ct<1499702400000")//查询某一天的push总量
    val dfActivityOneDay = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.activity?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner"))).where("ct> 1499616000000 and ct<1499702400000")//查询某一天的activity总量
    val dfDvOneDayWithCt = session.read.format("com.aerospike.spark.sql").option("aerospike.set", "dv").load.where("ct>1500134400 and ct<1500220800").filter("ct is not null");
    val dfDvOneDayWithMt = session.read.format("com.aerospike.spark.sql").option("aerospike.set", "dv").load.where("mt>1500134400000 and mt<1500220800000").filter("mt is not null");

    val appApp = dfApp.join(dfTdAppWithOutId,dfApp("_id")===dfTdAppWithOutId("app"))
    val appGame = dfApp.join(dfTdGameWithOutId,dfApp("_id")===dfTdGameWithOutId("app"))
    dfApp.createOrReplaceTempView("dfApp")
    dfTd.createOrReplaceTempView("dfTd")
    val appMpush = sqlContext.sql("select * from dfApp  where not exists (select * from dfTd WHERE dfApp._id=dfTd.app)")

    val pushCount_App = appApp.join(dfPushOneDay,appApp("_id")===dfPushOneDay("app")).count()//app每日贡献push数 4838
    val pushCount_Game = appGame.join(dfPushOneDay,appGame("_id")===dfPushOneDay("app")).count()//game每日贡献push数  1

    val activityCount_App = appApp.join(dfActivityOneDay, appApp("_id") === dfActivityOneDay("app")).count()//app每日贡献activity数 1
    val activityCount_Game = appGame.join(dfActivityOneDay,appGame("_id")===dfActivityOneDay("app")).count() //game每日贡献activity数 1

    val dvCount_App = appApp.join(dfDvOneDayWithCt,appApp("_id.oid")===dfDvOneDayWithCt("app")).count()// 每日贡献的新增设备数   3800
    val dvCount_Game =appGame.join(dfDvOneDayWithCt,appGame("_id.oid")===dfDvOneDayWithCt("app")).count()// 每日贡献的新增设备数  1200

    val activeDv_App = appApp.join(dfDvOneDayWithMt,appApp("_id.oid")===dfDvOneDayWithMt("app")).count()// 每日贡献的活跃设备数  3800
    val activeDv_Game =appGame.join(dfDvOneDayWithMt,appGame("_id.oid")===dfDvOneDayWithMt("app")).count()// 每日贡献的活跃设备数  1200

    val fileName = "statistics_result_" + getDt + ".txt"
    val writer = new PrintWriter(new File(fileName))
    writer.write("app每日贡献push数:"+pushCount_App+"\n")
    writer.write("game每日贡献push数:"+pushCount_Game+"\n")

    writer.write("app每日贡献activity数:"+activityCount_App+"\n")
    writer.write("game每日贡献activity数:"+activityCount_Game+"\n")

    writer.write("app每日贡献的新增设备数:"+dvCount_App+"\n")
    writer.write("game每日贡献的新增设备数:"+dvCount_Game+"\n")

    writer.write("app每日贡献的活跃设备数:"+activeDv_App+"\n")
    writer.write("game每日贡献的活跃设备数:"+activeDv_Game+"\n")
    writer.close()
  }
}
