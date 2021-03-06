package com.mongo.spark

import java.io.{FileSystem => _, _}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HDFSFileService {
  def getOneDayStart_time(date:Date):Long={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(date)).getTime
    val str = a+""
    str.toLong
  }
  def getOneDayEnd_time(date:Date):Long={
    var cal = Calendar.getInstance();
    cal.setTime(date);
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 999);
    val a = cal.getTime.getTime
    val str = a+""
    str.toLong
  }

  def getDt(date:Date) = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = dateFormat.format(date)
    dt
  }

  def getDaysBefore(dt: Date, interval: Int):Date = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);
    cal.add(Calendar.DATE, - interval)
    val day = cal.getTime()
    day
  }

  def getSparkSession(mongoUrl: String, sparkUrl:String,asSeedhost:String,asHost:String): SparkSession = {
    val uri: String = "mongodb://" + mongoUrl + "/mpush.app?readPreference=primary"
    val conf = new SparkConf()
      .setMaster("spark://" + sparkUrl)
      .setAppName("MongoSparkConnectorTour")
      .set("spark.app.id", "MongoSparkConnectorTour")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)
      .set("aerospike.seedhost", asSeedhost)
      .set("aerospike.port",  asHost)
      .set("aerospike.namespace", "push")
    val spark = SparkSession.builder().config(conf).master("spark://" + sparkUrl).appName("scalamongoas").getOrCreate()
    spark
  }

  //../spark-2.1.0-bin-hadoop2.7/bin/./spark-submit --class com.mongo.spark.HDFSFileService --master spark://172.23.5.113:7077 scalamongoas-assembly-1.3.1.jar
  //./spark-submit --class com.mongo.spark.HDFSFileService --master spark://172.23.5.113:7077 scalamongoas-assembly-1.3.1.jar
  def main(args: Array[String]): Unit = {
    //======读取配置文件start======
    val filePath =System.getProperty("user.dir")
    println("filePath:"+filePath)
    val postgprop = new Properties
    val ipstream = new BufferedInputStream(new FileInputStream(filePath+"/conf/config.properties"))
    postgprop.load(ipstream)
    //======读取配置文件end======
    val mongoUrl = postgprop.getProperty("mongo.userName")+":"+postgprop.getProperty("mongo.password")+"@"+postgprop.getProperty("mongo.host")
    val sparkUrl = postgprop.getProperty("spark.host") + ":" + postgprop.getProperty("spark.port")
    val session = getSparkSession(mongoUrl,sparkUrl,postgprop.getProperty("aerospike.seedhost"),postgprop.getProperty("aerospike.port"))
    val sqlContext = session.sqlContext
    val dfApp = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> ("mongodb://" + mongoUrl + "/mpush.app?readPreference=primary"), "partitioner" -> "MongoSplitVectorPartitioner")))
    val dfTd = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> ("mongodb://" + mongoUrl + "/mpush.td?readPreference=primary"), "partitioner" -> "MongoSplitVectorPartitioner")))
    val dfTdAppWithOutId = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> ("mongodb://" + mongoUrl + "/mpush.td?readPreference=primary"), "partitioner" -> "MongoSplitVectorPartitioner"))).where("source='app'").drop("_id")
    val dfTdGameWithOutId = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> ("mongodb://" + mongoUrl + "/mpush.td?readPreference=primary"), "partitioner" -> "MongoSplitVectorPartitioner"))).where("source='game'").drop("_id")

    for(a <- 1 to 7){
      var oneDay = getDaysBefore(new Date(),a)
      val oneDayStart_time = getOneDayStart_time(oneDay)
      val oneDayEnd_time = getOneDayEnd_time(oneDay)
      val dfPushOneDay = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> ("mongodb://" + mongoUrl + "/mpush.push?readPreference=primary"), "partitioner" -> "MongoSplitVectorPartitioner"))).where("ct>"+oneDayStart_time+" and ct<"+oneDayEnd_time)//查询某一天的push总量
      val dfActivityOneDay = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> ("mongodb://" + mongoUrl + "/mpush.activity?readPreference=primary"), "partitioner" -> "MongoSplitVectorPartitioner"))).where("ct>"+oneDayStart_time+" and ct<"+oneDayEnd_time)//查询某一天的activity总量
      val dfDvOneDayWithCt = session.read.format("com.aerospike.spark.sql").option("aerospike.set", "dv").load.filter("ct is not null and (tp='a' or (tp = 'i' and length(token) > 0)) and "+"ct>"+oneDayStart_time+" and ct<"+oneDayEnd_time);
      dfDvOneDayWithCt.cache
      val dfDvOneDayWithMt = session.read.format("com.aerospike.spark.sql").option("aerospike.set", "dv").load.filter("mt is not null and "+"mt>"+oneDayStart_time+" and mt<"+oneDayEnd_time);

      val appApp = dfApp.join(dfTdAppWithOutId,dfApp("_id")===dfTdAppWithOutId("app"))
      val appGame = dfApp.join(dfTdGameWithOutId,dfApp("_id")===dfTdGameWithOutId("app"))
      dfApp.createOrReplaceTempView("dfApp")
      dfTd.createOrReplaceTempView("dfTd")
      val appMpush = sqlContext.sql("select * from dfApp  where not exists (select * from dfTd WHERE dfApp._id=dfTd.app)")

      val dvCount_App = appApp.join(dfDvOneDayWithCt,appApp("_id.oid")===dfDvOneDayWithCt("app")).count()// 每日贡献的新增设备数   6321
      val dvCount_Game =appGame.join(dfDvOneDayWithCt,appGame("_id.oid")===dfDvOneDayWithCt("app")).count()// 每日贡献的新增设备数  1200
      val dvCount_Mpush =appMpush.join(dfDvOneDayWithCt,appMpush("_id.oid")===dfDvOneDayWithCt("app")).count()

      val pushCount_App = appApp.join(dfPushOneDay,appApp("_id")===dfPushOneDay("app")).count()//app每日贡献push数 4838
      val pushCount_Game = appGame.join(dfPushOneDay,appGame("_id")===dfPushOneDay("app")).count()//game每日贡献push数  1
      val pushCount_Mpush = appMpush.join(dfPushOneDay,appMpush("_id")===dfPushOneDay("app")).count()

      val activityCount_App = appApp.join(dfActivityOneDay, appApp("_id") === dfActivityOneDay("app")).count()//app每日贡献activity数 1
      val activityCount_Game = appGame.join(dfActivityOneDay,appGame("_id")===dfActivityOneDay("app")).count() //game每日贡献activity数 1
      val activityCount_Mpush = appMpush.join(dfActivityOneDay,appMpush("_id")===dfActivityOneDay("app")).count()

      val activeDv_App = appApp.join(dfDvOneDayWithMt,appApp("_id.oid")===dfDvOneDayWithMt("app")).count()// 每日贡献的活跃设备数  3800
      val activeDv_Game =appGame.join(dfDvOneDayWithMt,appGame("_id.oid")===dfDvOneDayWithMt("app")).count()// 每日贡献的活跃设备数  1200
      val activeDv_Mpush =appMpush.join(dfDvOneDayWithMt,appMpush("_id.oid")===dfDvOneDayWithMt("app")).count()

      val appAddCount = appApp.where("ct>"+oneDayStart_time+" and ct<"+oneDayEnd_time).count()  //app每日新增数  96
      val gameAddCount = appGame.where("ct>"+oneDayStart_time+" and ct<"+oneDayEnd_time).count() //game每日新增数     37
      val mpushAddCount = appMpush.where("ct>"+oneDayStart_time+" and ct<"+oneDayEnd_time).count()  // 0

      val appTotalCount = appApp.count()  //app累计数
      val gameTotalCount = appGame.count() //game累计数
      val mpushTotalCount = appMpush.count()  //mpush累计数

      val fileName = "statistics_result_" + getDt(oneDay) + ".txt"
      val writer = new PrintWriter(new File(fileName))
      writer.write("app每日贡献push数:"+pushCount_App+"\n")
      writer.write("game每日贡献push数:"+pushCount_Game+"\n")
      writer.write("mpush每日贡献push数:"+pushCount_Mpush+"\n")

      writer.write("app每日贡献activity数:"+activityCount_App+"\n")
      writer.write("game每日贡献activity数:"+activityCount_Game+"\n")
      writer.write("mpush每日贡献activity数:"+activityCount_Mpush+"\n")

      writer.write("app每日贡献的新增token数:"+dvCount_App+"\n")
      writer.write("game每日贡献的新增token数:"+dvCount_Game+"\n")
      writer.write("mpush每日贡献的新增token数:"+dvCount_Mpush+"\n")

      writer.write("app每日贡献的活跃设备数:"+activeDv_App+"\n")
      writer.write("game每日贡献的活跃设备数:"+activeDv_Game+"\n")
      writer.write("mpush每日贡献的活跃设备数:"+activeDv_Mpush+"\n")

      writer.write("app每日新增数:"+appAddCount+",累计数:"+appTotalCount+"\n")
      writer.write("game每日新增数:"+gameAddCount+",累计数:"+gameTotalCount+"\n")
      writer.write("mpush每日新增数:"+mpushAddCount+",累计数:"+mpushTotalCount+"\n")
      writer.close()
    }
  }
}
