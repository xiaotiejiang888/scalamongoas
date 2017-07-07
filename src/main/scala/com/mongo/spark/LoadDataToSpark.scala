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
      var tds = conn("mpush")(table).find
      val fileName = table + "_" + dt + ".txt"
      var tdsStr = "";
      for (elem <- tds) {
        tdsStr += (elem.toString+"\n")
      }
      HDFSFileService.saveFileFromInputStream(new ByteArrayInputStream(tdsStr.getBytes()),fileName)
    }
    conn.close
  }
}

