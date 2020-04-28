package Exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source

object rdd extends App{
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("RDD first steps")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

//  case class Stock (name: String, date: String, price: Double)
//
//  val csvText = Source.fromFile("src/main/resources/data/stocks.csv")
//    .getLines().
//    drop(1)
//    .map(x => x.split(","))
//    .map(y => Stock(y(0), y(1), y(2).toDouble))
//    .toList
//
//  val stockRDD = sc.parallelize(csvText)

  val movie = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
      .select("Title", "Major_genre","IMDB_Rating")
    .where(col("Major_genre").isNotNull and col("IMDB_Rating").isNotNull)

  case class Movie(Title: String, Major_genre: String, IMDB_Rating: Double)

  import spark.implicits._
  val moviesDS = movie.as[Movie]

  //1
  val movieRDD = moviesDS.rdd

  //2
  val genres = movieRDD.map(_.Major_genre).distinct()

  //3
  val dramaAndGood = movieRDD.filter(_.Major_genre == "Drama")
    .filter(_.IMDB_Rating > 6)

  case class GenreAvgRating (genre: String, rating: Double)

  val avgRating = movieRDD.groupBy(_.Major_genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.IMDB_Rating).sum / movies.size)
  }
  avgRating.toDF.show()

  movieRDD.toDF.groupBy("Major_genre").agg(avg(col("IMDB_Rating"))).show()








}
