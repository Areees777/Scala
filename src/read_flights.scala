import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object read_flights {
  def main (args: Array[String]): Unit ={

    val file1 = "D:/Spark_Maven/src/data/flights_parquet"

    val sparkSession = SparkSession.builder
      .appName("Flights")
      .master("local")
      .getOrCreate()

    //val df_1 = sparkSession.read.option("header","true").parquet(file1)
    val DS_flights = sparkSession.read.format("parquet")
        .load(file1)
    //En este momento flights es un Dataset.

    DS_flights.show()

    // 1. Cuenta el numero de vuelos por año
    DS_flights
      .groupBy(col("Year"))
      .count()
      .show()

    // 2. Top 5 de vuelos de salida por aeropuerto
    //Queremos saber de que origenes hay más vuelos
    DS_flights
      .groupBy(col("Origin"))
      .count()
      .show(5)


    // 3. Top 5 en retrasos en aeropuertos en abril
    //Lo mismo que el anterior, pero ahora queremos solo los que salieron en abril
    DS_flights
        .filter(col("Month") === 4)
      .groupBy(col("Origin"))
      .count()
      .show(5)





  }
}
