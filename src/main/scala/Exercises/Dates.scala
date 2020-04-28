package Exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Dates extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("Dates in practice")
    .config("spark.master", "local")
    .getOrCreate()

  val movies = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  val movDates = movies.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Parsed_date")
  ).show()
}
