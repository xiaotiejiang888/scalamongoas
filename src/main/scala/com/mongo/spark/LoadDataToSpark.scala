import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import com.mongo.spark.HDFSFileService


object LoadDataToSpark {
  def main(args: Array[String]): Unit = {
    var tables = Array("td","app","push","activity")
    def getDt = {
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dt = dateFormat.format(now)
      dt
    }
    val dt: String = getDt
    val conn = MongoFactory.getConnection
    for (table <- tables){
      val start = new Date().getTime
      var tds = conn("mpush")(table).find().skip(0).limit(5000)
      val endQueryTime = new Date().getTime
      println(table+"查询耗时："+(endQueryTime-start)+"ms")
      try{
        val fileName = table + "_" + dt + ".txt"
        val file = new File(fileName)
        val writer = new PrintWriter(file)
        var tdsStr = "";
        while(tds.hasNext){
          tdsStr += (tds.next().toString+"\n")
        }
        val endPinStrTime = new Date().getTime
        println(table+"写入拼str耗时："+(endPinStrTime-endQueryTime)+"ms")
        writer.write(tdsStr)
        writer.close()
        val endLocalTime = new Date().getTime
        println(table+"写入本地文件耗时："+(endLocalTime-endPinStrTime)+"ms")
        HDFSFileService.saveFile(fileName)
        println(table+"保存到hdfs耗时："+(new Date().getTime-endLocalTime)+"ms")
        file.delete()
        println(table+"处理完毕")
      }catch {
        case e: Exception => println("exception caught: " + e);
      }
    }
    conn.close
  }
}

