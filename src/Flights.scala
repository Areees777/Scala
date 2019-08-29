//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Flights {
  def main (args: Array[String]): Unit ={
    val file1 = "D:/Spark_Maven/src/data/2007.csv"
    val file2 = "D:/Spark_Maven/src/data/2008.csv"

    val sparkSession = SparkSession.builder
      .appName("Flights")
      .master("local")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val flights_2007 = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema","true")
      .option("multiLine",true)
      .option("delimiter",",")
      .load(file1)

    val flights_2008 = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema","true")
      .option("multiLine",true)
      .option("delimiter",",")
      .load(file2)

    val flights2007_2008 = flights_2007.union(flights_2008)


    flights2007_2008.write.parquet("D:/Spark_Maven/src/data/flights_parquet")



  }

}
