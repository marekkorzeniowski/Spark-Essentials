package Taxi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaxiApp extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("TaxiApp")
    .getOrCreate()

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
//  taxiDF.printSchema()



  val taxiZonesDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

//  taxiZonesDF.printSchema()
//  taxiZonesDF.show()
  /**
    * Questions:
    *
    * 1. Which zones have the most pickups/dropoffs overall?
    * 2. What are the peak hours for taxi?
    * 3. How are the trips distributed by length? Why are people taking the cab?
    * 4. What are the peak hours for long/short trips?
    * 5. What are the top 3 pickup/dropoff zones for long/short trips?
    * 6. How are people paying for the ride, on long/short trips?
    * 7. How is the payment type evolving with time?
    * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
    *
    */

    //1
  val pickUp = taxiDF.groupBy(col("PULocationID"))
      .agg(count("*").as("total_trips"))
      .join(taxiZonesDF, taxiDF.col("PULocationID") === taxiZonesDF.col("LocationID"))
      .drop("PULocationID", "service_zone")
      .orderBy(col("total_trips").desc_nulls_last)

//  pickUp.show()

  //2
  val peakHours = taxiDF
    .withColumn("Hour", hour(col("tpep_pickup_datetime")))
    .groupBy(col("Hour"))
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)

//  peakHours.show()

  //3
  val distanceDF = taxiDF.select(col("trip_distance"))
  val longDistance = 20

  val distanceDistribution = distanceDF.select(
    count("*").as("total_trips"),
    avg(col("trip_distance")).as("Avg_distance"),
    stddev(col("trip_distance")).as("Standard_deviation"),
    lit(longDistance).as("Long_distance")
  )

//  distanceDistribution.show()

  //4
  val peakHoursLongDistance = taxiDF
    .filter(col("trip_distance") > longDistance)
    .withColumn("Hour", hour(col("tpep_pickup_datetime")))
    .groupBy(col("Hour"))
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)

//  peakHoursLongDistance.show()

//* 5. What are the top 3 pickup/dropoff zones for long/short trips?
  val top3pickUp = taxiDF.filter(col("trip_distance") > longDistance)
    .groupBy(col("PULocationID"))
    .agg(count("*").as("total_amount"))
    .join(taxiZonesDF, taxiDF.col("PULocationID") === taxiZonesDF.col("LocationID"))
    .orderBy(col("total_amount").desc_nulls_last)
    .limit(3)
//      .join(taxiZonesDF, taxiDF.col("PULocationID") === taxiZonesDF.col("LocationID"))

//  top3pickUp.show()

  //* 6. How are people paying for the ride, on long/short trips?
  val paymentType = taxiDF
      .filter(col("trip_distance") > longDistance)
    .groupBy(col("payment_type"))
    .agg(count("*").as("All"))
//      .orderBy(col("All").desc_nulls_last)

//    paymentType.show()
//  tpep_pickup_datetime
//  PULocationID

  //7. How is the payment type evolving with time? - ???

  //8
  val carSharing = taxiDF
      .select(col("tpep_pickup_datetime"), col("total_amount"), col("PULocationID"))
    .withColumn("fiveMinId", round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer"))
      .groupBy(col("fiveMinId"), col("PULocationID"))
      .agg(count(col("*")).as("trips"), sum(col("total_amount")).as("total_amount"))
      .orderBy(col("trips").desc_nulls_last)
      .withColumn("approx_datetime", from_unixtime(col("fiveMinId") * 300))
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .drop("fiveMinId", "PULocationID", "LocationID", "service_zone")



//  carSharing.show()

  import spark.implicits._
  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  println(avgCostReduction)

val economicImpact = carSharing
  .withColumn("groupedRides", col("trips") * percentGroupAttempt)
  .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
  .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
  .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

  val totalImpact = economicImpact.select(sum(col("totalImpact")).as("total"))

  totalImpact.show()

//  9.537882853785204
//  +-----------------+
//  |            total|
//  +-----------------+
//  |39987.73868641883|


}


