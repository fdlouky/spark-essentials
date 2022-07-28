package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") // same as the following. To solve spark 3.0 date parse
  //  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesDF = spark.read
    .option("inferSchema", "false")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF
    .select(
      col("Title"),
      to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"), // conversion
    )

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

  moviesWithReleaseDates
    .select("*")
    .where(col("Actual_Release").isNull)

  /*
    Exercise
    1. How do we deal with multiple date formats?
    2. Read the stocks DF and parse the dates
   */

  // 1 - parse the DF multiple times, then union the small DFs
  // or use when clause
  moviesDF.select(col("Title"),
    when(to_date(col("Release_Date"), "dd-MMM-yy").isNotNull,
      date_format(to_date(col("Release_Date"), "dd-MMM-yy"), "MM/dd/yyyy"))
      .when(to_date(col("Release_Date"), "yyy-MM-dd").isNotNull,
        date_format(to_date(col("Release_Date"), "yyy-MM-dd"), "MM/dd/yyyy"))
      .otherwise("Unknown Format").as("Formatted Date")
  )

  // 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  // Structures

  // 1 - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")) // Profit -> [146083, 146083]
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF
    .select(
      col("Title"),
      split(col("Title"), " |,").as("Title_Words") // ARRAY of strings
    )// " |," -> split by separator space or comma

  moviesWithWords
    .select(
      col("Title"),
      expr("Title_Words[0]"), // indexing
      size(col("Title_Words")), // array size
      array_contains(col("Title_Words"), "Love"), // look for value in array
    )
}