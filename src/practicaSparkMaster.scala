import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window

object practicaSparkMaster {
  def main (args: Array[String]): Unit ={
    setupLogging()

    val sparkSession = SparkSession.builder
      .appName("Flights")
      .master("local")
      .getOrCreate()

    val movies = sparkSession.read.text("XXX/movies.dat")

    val moviesDF = movies
      .select(split(col("value"),"::").as("Split"))
      .withColumn("movieId",col("Split").getItem(0))
      .withColumn("title",col("Split").getItem(1))
      .withColumn("genres",col("Split").getItem(2))
      .drop(col("Split"))
    moviesDF.show()

    case class Movie(movieId: Int, title: String, genres: String)

    val ratings = sparkSession.read.text("XXX/ratings.dat")

    val ratingsDF = ratings
      .select(split(col("value"),"::").as("Split"))
      .withColumn("userId",col("Split").getItem(0))
      .withColumn("movieId",col("Split").getItem(1))
      .withColumn("rating",col("Split").getItem(2))
      .withColumn("timestamp",col("Split").getItem(3))
      .drop(col("Split"))
    ratingsDF.show()

    case class Rating(userId: Int, movieId: Int, rating: Int, timestamp: Int)

    val users = sparkSession.read.text("XXX/users.dat")

    val usersDF = users
      .select(split(col("value"),"::").as("Split"))
      .withColumn("userId",col("Split").getItem(0))
      .withColumn("gender",col("Split").getItem(1))
      .withColumn("age",col("Split").getItem(2))
      .withColumn("occupation",col("Split").getItem(3))
      .withColumn("zipCode",col("Split").getItem(4))
      .drop(col("Split"))
    //usersDF.show()

    case class User(userId: Int, gender: String, age: Int, occupation: Int, zipCode: Int)

    // Muestra el schema de Users y lista los 20 primeros elementos del dataframe
    //usersDF.printSchema()

    // Registra una tabla temporal para los datos de User, Ratings y otra para la de Movies
    usersDF.createOrReplaceTempView("users")
    ratingsDF.createOrReplaceTempView("ratings")
    moviesDF.createOrReplaceTempView("movies")

    // Aprovechando los Dataframes que hemos creado y las funciones de Spark SQL, vamos a realizar una serie
    // de consultas para familiarizarnos con los datos:
    //Analizando el Dataset de Ratings obtén la mayor y la menor valoración así como el número de usuarios que
    // dieron esas valoraciones.
    ratingsDF
      .groupBy(col("rating"))
      .agg(count("*").as("NumRatings"))
      .filter(col("rating") === 5 || col("rating") === 1)
      .show()

    // Lista los 10 usuarios más activos y el número de veces que valoraron una película
    // Tomo que usuarios más activos serán los que han hecho mayor numero de valoraciones
    ratingsDF
      .groupBy(col("userId"))
      .agg(count("*").as("ratingsByUser")).orderBy(col("ratingsByUser").desc)
      .limit(10)
      .show()

    // Muestra las películas que el usuario 4169 valoró con más de 4
    val joinRatingsUsers = ratingsDF
      .join(moviesDF,"movieId")

    joinRatingsUsers
      .filter(col("userId") === 4169 && col("rating").gt(4))
      .select(col("userId"),col("title"),col("genres"),col("rating"))
      .show()










  }
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }






}


