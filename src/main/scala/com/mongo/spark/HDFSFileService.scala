package com.mongo.spark

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

object HDFSFileService {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def saveFile(filepath: String): Unit = {
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
  }

  def saveFileFromInputStream(inputStream: InputStream, fileName:String): Unit = {
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

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MongoSparkConnectorTour").master("spark://172.23.5.113:7077")
      .getOrCreate()
    val df = spark.sqlContext.read.json("hdfs://wifianalytics/user/zmy/td_2017-07-06.txt");
    df.show();
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
}
