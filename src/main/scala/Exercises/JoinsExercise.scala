package Exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, _}



object JoinsExercise extends App{
  Logger.getLogger("org").setLevel(Level.OFF)


  val spark = SparkSession.builder()
    .appName("JoinsExer")
    .config("spark.master", "local")
    .getOrCreate()

  def readDF(name: String) = {
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "public."+ name)
      .load()
  }

  val employeesDF =readDF("employees")
  val salaryDF = readDF("salaries")
  val titlesDF = readDF("titles")
  val managersDF = readDF("dept_manager")

  // 1
  val maxSalary = salaryDF.groupBy("emp_no").agg(max("salary").as("MaxSalary"))
  val mergedEmplMaxSal = employeesDF.join(maxSalary, "emp_no")

//  mergedEmplMaxSal.show(10)

  //2
  val neverManagers = employeesDF.join(managersDF,
    employeesDF.col("emp_no") === managersDF.col("emp_no"), "left_anti")

//  neverManagers.show(10)

  //3
  val mostRecentJobs = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmp = maxSalary.orderBy(col("MaxSalary").desc).limit(10)
0
  val bestTitles = bestPaidEmp.join(mostRecentJobs, "emp_no")

  bestTitles.show(10)


}
