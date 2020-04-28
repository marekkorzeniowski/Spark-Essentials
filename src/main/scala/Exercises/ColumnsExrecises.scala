package Exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsExrecises extends App{
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .appName("Columns exercises")
    .config("spark.master", "local")
    .getOrCreate()
  import spark.implicits._

  val movies = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
    .select("Title", "IMDB_Rating", "US_DVD_Sales", "US_Gross", "Worldwide_Gross","Release_Date", "Major_Genre", "Director")
      .na.fill(0)

  movies.printSchema()


  val date = movies
    .select($"Title",lpad($"Release_Date",9, "0").as("newDate"))
    .withColumn("day_month",substring($"newDate", 1, 7))
    .withColumn("year",substring($"newDate", 8, 2))
    .withColumn("correct_year", when($"year" >= 20, lpad($"year", 4, "19"))
    .otherwise(lpad($"year", 4, "20")))
    .withColumn("correctDate", concat(col("day_month"), col("correct_year")))
    .withColumn("Parsed_Date", to_date($"correctDate", "dd-MMM-yyyy"))

  val title_date = date.select(col("Title"), col("Parsed_Date"))

//  date.printSchema()
//  date.show()

//title_date.show()

  val mergedDFs = movies.join(title_date, Seq("Title"), "Inner")
    .withColumn("summary_revenue", $"US_DVD_Sales" + $"US_Gross" + $"Worldwide_Gross")
//    .filter($"IMDB_Rating" > 6)
//    .filter($"Major_Genre" === "Comedy")

  val total_profit = mergedDFs.withColumn("Income", lit("Total"))
    .groupBy(col("Income"))
    .sum("summary_revenue")

  //using aliases
//  mergedDFs.select(countDistinct(col("Major_genre")).as("Total")).show()

movies.groupBy("Director")
    .agg(
      avg("IMDB_Rating"),
        avg("US_Gross")
    ).show()
















//  date.show()


//  mov.show(10)
//  mov2.show(10)
//  mov2.printSchema()


}
