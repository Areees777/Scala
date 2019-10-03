import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window


object kddcup {
  def main (args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("KddCup")
    val sc = new SparkContext(conf)

    val data_file = "C:/Users/aargiles/IdeaProjects/Spark_Training/Scala/src/main/scala/kddcup.data_10_percent_corrected"
    val raw_rdd = sc.textFile(data_file).cache()
    //raw_rdd.take(5).foreach(println)

    val csv_rdd = raw_rdd.map(row => row.split(","))

    csv_rdd.take(1).foreach(println)
  }
}
