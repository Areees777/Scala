import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object tarjetasBlack {

  def main (args: Array[String]): Unit ={
    val file = "D:/Spark/src/data/BlackFriday.csv"
    val file2 = "D:/Spark/src/data/tarjetasBlack2.csv"

    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Training App")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()

    val black = sparkSession.read.format("csv")
        .option("header", "true")
        .option("inferSchema","true")
        .option("multiLine",true)
        .option("delimiter",",")
        .load(file2)

    black.show()

    //Los 10 movimientos más caros
    black
      .select(col("id_movimiento"),col("id_miembro"),col("importe"))
      .orderBy(col("importe").desc)
      .show(10)

    //El miembro que más ha gastado
    black
      .groupBy(col("id_miembro"))
      .agg(sum("importe").as("Suma"))
      .orderBy(col("Suma").desc)
      .show()


    //La actividad que más ha gastado
    black
      .groupBy(col("actividad"))
      .agg(sum("importe").as("Suma_Actividad"))
      .orderBy(col("Suma_Actividad").desc)
      .show()


    //Mostrar todas las actividades que contengan la palabra "Restaurante"
    black
      .filter(col("actividad").contains("RESTAURANTE"))
      .show()

    //Sustituir de la columna fecha los "-" por "/"
    def udf_fecha: UserDefinedFunction = udf {changeDate}

    def changeDate: (String) => String = {
      fecha: String =>
        fecha.replace('-', '/')
    }

    val black_UDF = black.withColumn("fecha", udf_fecha(col("fecha")))

    black_UDF.show


    //Hacer un split de la columna fecha por el caracter "-" y crear 3 columnas nuevas con esos datos
    black_UDF
      .select(split(col("fecha"),"/").as("Split"))
      .withColumn("Year",col("Split").getItem(0))
      .withColumn("Month",col("Split").getItem(1))
      .withColumn("Day",col("Split").getItem(2))
      .show()

    //Elevar al cuadrado cada valor de la columna importe y guardarlo en otra columna
    val importe2 = pow(col("importe"),2)
    black
      .select(col("id_movimiento"),col("importe"), importe2.alias("Importe2"))
      .show()


    //Crear una columna en función del importe 0 si es menor de 500 y 1 si es menor de 1000, 2 lo demás.
    black
      .withColumn("importe2", when(col("importe") <500 , 0)
        .when(col("importe") < 1000,1).otherwise(2))
      .show()

    //Usar el MAP con una columna "complex_map"
    black
      .select(map(col("actividad"),col("importe")).alias("complex_map"))
      .selectExpr("complex_map['RESTAURANTE']")
      .show()




  }
}
