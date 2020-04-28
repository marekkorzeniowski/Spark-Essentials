package Exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object DataSets extends App{
  Logger.getLogger("org").setLevel(Level.OFF)


  val spark = SparkSession.builder()
    .appName("dataSets")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
              )
  import spark.implicits._

  val carsDS = carsDF.as[Car]

  val avgHP = carsDS
    .map(x => x.Horsepower.getOrElse(0L))
    .reduce(_ + _) / carsDS.count()

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")


  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    * (hint: use array_contains)
    */


  val guitarAndPlayer = guitarPlayersDS
    .joinWith(guitarsDS,
    array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
    "outer")
    .show()
}
