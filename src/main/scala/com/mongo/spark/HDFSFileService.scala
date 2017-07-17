package com.mongo.spark

import java.io.{FileSystem => _, _}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.mongodb.spark.config._
import com.mongodb.spark.sql._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object HDFSFileService {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def saveFile(filepath: String): Unit = {
    try{
      val file = new File(filepath)
      val out = fileSystem.create(new Path(file.getName))
      val in = new BufferedInputStream(new FileInputStream(file))
      var b = new Array[Byte](1024)
      var numBytes = in.read(b)
      while (numBytes > 0) {
        out.write(b, 0, numBytes)
        numBytes = in.read(b)
      }
      in.close()
      out.close()
    }catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

  def saveFileFromInputStream(inputStream: InputStream, fileName:String): Unit = {
    try{
      val out = fileSystem.create(new Path(fileName))
      val in = new BufferedInputStream(inputStream)
      var b = new Array[Byte](1024)
      var numBytes = in.read(b)
      while (numBytes > 0) {
        out.write(b, 0, numBytes)
        numBytes = in.read(b)
      }
      in.close()
      out.close()
    }catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  def getOneDayStart_time():Long={
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    var str = a+""
    str.toLong
  }
  def getOneDayEnd_time():Long={
    var cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 999);
    val a = cal.getTime.getTime
    var str = a+""
    str.toLong
  }

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(args)
    val sqlContext = SQLContext.getOrCreate(sc)
    val dfApp = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.app?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner")))
    val dfTd = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.td?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner")))
    val dfTdApp = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.td?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner"))).where("source='app'")
    val dfTdAppWithOutId = dfTdApp.drop("_id")
    val dfTdGame = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.td?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner"))).where("source='game'")
    val dfTdGameWithOutId = dfTdGame.drop("_id")
    val dfPush = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.push?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner")))
    val dfActivity = sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://zmy3:zmy3@172.23.5.158/mpush.activity?readPreference=primary", "partitioner" -> "MongoSplitVectorPartitioner")))
    val sprkAsconf = new SparkConf().setMaster("spark://172.23.5.113:7077").setAppName("Aerospike Tests for Spark Dataset").set("aerospike.seedhost", "172.23.5.158").set("aerospike.port",  "3000").set("aerospike.namespace", "push")
    val spark = SparkSession.builder().config(sprkAsconf).master("spark://172.23.5.113:7077").appName("Aerospike Tests").getOrCreate()
    val dfDv = spark.read.format("com.aerospike.spark.sql").option("aerospike.set", "dv").load
    var appApp = dfApp.join(dfTdAppWithOutId,dfApp("_id")===dfTdAppWithOutId("app"))
    var appGame = dfApp.join(dfTdGameWithOutId,dfApp("_id")===dfTdGameWithOutId("app"))
    dfApp.registerTempTable("dfApp")
    dfTd.registerTempTable("dfTd")
    var appMpush = spark.sqlContext.sql("select * from dfApp  where _id not in (select app from dfTd)")
    var count = appApp.join(dfPush,appApp("_id")===dfPush("app")).select(dfPush("_id"),dfPush("ct")).where("ct> 1499616000000 and ct< 1499702400000").count()
    val fileName = "统计结果_" + getDt + ".txt"
    val writer = new PrintWriter(new File(fileName))
    writer.write("app每日贡献push数:"+count)
    writer.close()
  }
  def getDt = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dt = dateFormat.format(now)
    dt
  }
  def getSparkContext(args: Array[String]): SparkContext = {
    val uri: String = args.headOption.getOrElse("mongodb://zmy3:zmy3@172.23.5.158/mpush.app?readPreference=primary")
    val conf = new SparkConf()
      .setMaster("spark://172.23.5.113:7077")
      .setAppName("MongoSparkConnectorTour")
      .set("spark.app.id", "MongoSparkConnectorTour")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val sc = new SparkContext(conf)
    sc
  }
}
