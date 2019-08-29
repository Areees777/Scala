import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


object Pokemon {
  def main (args: Array[String]): Unit ={
    val file = "D:/Spark/src/data/Pokemon.csv"

    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Training App")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()

    val pokemon = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema","true")
      .option("multiLine",true)
      .option("delimiter",",")
      .load(file)

    pokemon.show()

    //Sacar el pokemon con mejor ataque
    pokemon
      .select(col("*"))
      .orderBy(col("Attack").desc)
      .show(1)

    //Sacar los 5 mejores pokemon por total
    pokemon
      .select(col("*"))
      .orderBy(col("Total").desc)
      .show(5)

    //Sacar los 5 mejores pokemon por total y por tipo
    //pokemon.groupBy(col("Type 2")).agg(max("Total")).show()

    val w = Window.partitionBy(col("Type 2")).orderBy(col("Total").desc)

    val dfTop = pokemon
      .withColumn("rn", row_number.over(w))
      .where(col("rn") === 1).drop(col("rn"))
    dfTop.show()

    pokemon
      .select(col("*"))
      .where(col("Type 2") === "Water")
      .orderBy(col("Total").desc)
      .show()


  }


}
