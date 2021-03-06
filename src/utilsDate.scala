
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.LocalDate
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Utils_Date {
  
  private val dateFmt = "yyyy-MM-dd"
  def currentHour(): String = {
    
    var startingTime = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toString()
    
    return startingTime
  }
  

  def getTodayDate(fecha: String): String = {
    if (fecha.length() == 0){
      val date = new Date
      val sdf = new SimpleDateFormat(dateFmt)
      return sdf.format(date)     
    }    
    return fecha 
  }

  def getYesterdayDate(fecha: String): String = {
    if (fecha.length() == 0){
      val calender = Calendar.getInstance()
      calender.roll(Calendar.DAY_OF_YEAR, -1)
      val sdf = new SimpleDateFormat(dateFmt)
      return sdf.format(calender.getTime())
    }    
    return fecha
    
  }
