package part3typesdatasets

import org.apache.spark.sql.functions.{array_contains, avg, col, sum, to_date}
import org.apache.spark.sql._

import java.sql.Date

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read // type DataFrame = Dataset[Row] -> DataFrame is an Alias of Dataset. So Dataset and Dataframes share methods and fields
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema() //numbers: integer (nullable = true)

  // datasets are more likely scala models -> we can use all functional programming approaches that scala brings: map, flatMap, filter, etc

  // convert a DF to a Dataset
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS = numbersDF.as[Int]

  // dataset if a complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Double],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Fab_Date: Date,
                  Origin: String
                )

  // 2 - read th DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")
  val carsWithFabDateDF = carsDF.withColumn("Fab_Date", to_date(col("Year"), "yyyy-MM-dd"))


  // 3 - define an encoder (importing the implicits)

  import spark.implicits._

  // 4 - convert the DF to DS
  val carsDS = carsWithFabDateDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNameDS = carsDS.map(car => car.Name.toUpperCase())

  /*
    Exercises
    1. Count how many cars we have
    2. Count how many POWERFUL cars we have (HP > 140)
    3. Average HP for the entire dataset
   */

  // 1
  val carsCount = carsDS.count
  println(carsCount) // 406

  // 2
  println(carsDS.filter(_.Horsepower.getOrElse(0.0) > 140).count()) // 81
  // getOrElse is because is Optional (could contain nulls). 0L -> zero long

  // 3
  println(carsDS.map(_.Horsepower.getOrElse(0.0)).reduce(_ + _) / carsCount) // 103.5295566502463

  // also use the DF Functions!
  carsDS.select(avg(col("Horsepower"))) // 105.0825

  // why result from line 84 is different of line 81?
  println(carsDS.map(_.Horsepower.getOrElse(0.0)).reduce(_ + _)) // 42033.0
  carsDS.select(sum(col("Horsepower"))) // 42033
  // 42033/105.0825 = 400 -> there are 6 rows with "Horsepower":null
  // so avg here is not considering rows with null values

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS
    .joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  //  guitarPlayerBandsDS.show() -> a Dataset of tuple of two Dataset

  /*
    Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    (hint: use array_contains)
   */

  val guitarPlayersGuitarsDS = guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")

  // Grouping DS
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations

}
