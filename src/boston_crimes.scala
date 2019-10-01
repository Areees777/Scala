import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


//Import de MongoDB
import org.mongodb.scala._

object boston_crimes {
  def main (args: Array[String]): Unit ={

    //Para conectar con la base de datos MongoDB
    //val uri: String = "mongodb+srv://<username>:<password>@<cluster-address>/test?retryWrites=true&w=majority"
    //System.setProperty("org.mongodb.async.type", "netty")
    //val client: MongoClient = MongoClient(uri)
    //val db: MongoDatabase = client.getDatabase("test")

    val file1 = "C:/Users/ares/IdeaProjects/Spark_Training/Scala/src/offense_codes.csv"
    val file2 = "C:/Users/ares/IdeaProjects/Spark_Training/Scala/src/crime.csv"

    val sparkSession = SparkSession.builder
      .appName("Flights")
      .master("local")
      .getOrCreate()

    val codes = sparkSession.read.textFile(file1)

    def udf_code: UserDefinedFunction = udf {changeCode}

    def changeCode: (String) => String = {
      code: String =>
        code.replace(',', ';')
    }

    val codesUDF = codes.withColumn("value", udf_code(col("value")))


    val DataFrameCodes = codesUDF
      .select(split(col("value"),";").as("Split"))
      .withColumn("OFFENSE_CODE",col("Split").getItem(0))
      .withColumn("NAME",col("Split").getItem(1))
      .drop(col("Split"))

    val codesDF = DataFrameCodes.filter(col("OFFENSE_CODE") =!= "CODE" && col("NAME") =!= "NAME")

    //codesDF.show()

    val crimes = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema","true")
      .option("multiLine",true)
      .option("delimiter",",")
      .load(file2)

    //crimesDF.show()

    val crimesData = crimes.select(
      split(col("OCCURRED_ON_DATE"), " ")(0).alias("OCCURRED_DAY"),
      split(col("OCCURRED_ON_DATE"), " ")(1).alias("OCCURRED_HOUR"),
      col("INCIDENT_NUMBER"),
      col("OFFENSE_CODE"),
      col("OFFENSE_CODE_GROUP"),
      col("OFFENSE_DESCRIPTION"),
      col("DISTRICT"),
      col("REPORTING_AREA"),
      col("SHOOTING"),
      col("YEAR"),
      col("MONTH"),
      col("DAY_OF_WEEK"),
      col("HOUR"),
      col("UCR_PART"),
      col("STREET")

    )
    val joinCodeCrimes = crimesData.join(codesDF,"OFFENSE_CODE")

    val crimesDF = joinCodeCrimes.withColumn("Week",date_format(to_date(col("OCCURRED_DAY")),"W"))

    crimesDF.show()

    //Qué tipo de crimenes son más comunes?
    //crimesFrequency(crimesDF)

    //Frecuencia de crimenes por dia
    //crimesFrecuencyByDay(crimesDF)

    //Frecuencia de crimenes por semana
    crimesFrecuencyByWeek(crimesDF)

    //Frecuencia de crimenes por año
    //crimesFrecuencyByYear(crimesDF)
  }

  def crimesFrequency(joinCodeCrimes: DataFrame) = {
    //joinCodeCrimes
    //  .groupBy(col("NAME")).count()
    //  .show()

    //EL anterior está bien, pero si lo que se quiere es poner un nombre a la columna se puede hacer
    //de la siguiente manera:

    joinCodeCrimes
      .groupBy(col("NAME"))
      .agg(count("*").as("Numero")).orderBy(col("Numero").desc)
      .show()
  }

  def crimesFrecuencyByDay(joinCodeCrimes: DataFrame) = {
    joinCodeCrimes
      .select(col("OFFENSE_CODE"),col("DAY_OF_WEEK"),col("NAME"))
      .groupBy(col("DAY_OF_WEEK"))
      .agg(count("*").as("Numero")).orderBy(col("Numero").desc)
      .show()
  }

  def crimesFrecuencyByWeek(joinCodeCrimes: DataFrame) = {
    joinCodeCrimes
      .select(col("OFFENSE_CODE"),col("WEEK"),col("YEAR"), col("NAME"))
      .groupBy(col("WEEK"),col("YEAR"))
      .agg(count("*").as("Numero")).orderBy(col("Numero").desc)
      .show()
  }

  def crimesFrecuencyByYear(joinCodeCrimes: DataFrame) = {
    joinCodeCrimes
      .select(col("OFFENSE_CODE"),col("YEAR"),col("NAME"))
      .groupBy(col("YEAR"))
      .agg(count("*").as("Numero")).orderBy(col("Numero").desc)
      .show()
  }

  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}
