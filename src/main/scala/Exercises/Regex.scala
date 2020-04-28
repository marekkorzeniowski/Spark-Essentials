package Exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Regex extends App {
  Logger.getLogger("org").setLevel(Level.OFF)


  val spark = SparkSession.builder()
    .appName("regexExer")
    .config("spark.master", "local")
    .getOrCreate()

  val cars = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()
  val carsNames = List("volkswagen", "vw", "volvo", "mercedes", "bmw")
  val regrexString = carsNames.mkString("|")

  cars.select(col("Name"),
    regexp_extract(col("Name"), regrexString, 0).as("Extract"))
    .where(col("Extract") =!= "")

  //version 2 with contains !!!
  val intialFilter = carsNames.map(_.toLowerCase).map(name => col("Name").contains(name))
  val secondaryFilter  = intialFilter.fold(lit(false))((x, y) => x or y)

  cars.filter(secondaryFilter).show()







}
