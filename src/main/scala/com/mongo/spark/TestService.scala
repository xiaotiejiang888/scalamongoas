import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object TestService {
  def getDaysBefore(dt: Date, interval: Int):Date = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);

    cal.add(Calendar.DATE, - interval)
    val day = cal.getTime()
    day
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
  def getOneDayStart_time(date:Date):Long={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(date)).getTime
    val str = a+""
    str.toLong
  }
  def main(args: Array[String]) {
    for( a <- 1 to 7){
      println(getDaysBefore(new Date(),a))
    }
    println(getOneDayEnd_time(new Date()))
    println(getOneDayStart_time(new Date()))
  }
}