 package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object DataFramesBasics extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DAtaFramesBasics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a DataFrame (DF)
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true") // WARNING! In production you shouldn't infer Schema
    .load("src/main/resources/data/cars.json")

  // showing a DF
  firstDF.show()
  firstDF.printSchema()

  // get rows
  firstDF.take(10).foreach(println) // each one is an Array[T] -> [12.0,8,307.0,130,18.0,chevrolet chevelle malibu,USA,3504,1970-01-01]

  // spark types
  val longType = LongType

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // obtain a schema
  val carsDFSchema = firstDF.schema
  println(carsDFSchema) // StructType(StructField(Acceleration,DoubleType,true), StructField(Cylinders,LongType,true), StructField(Displacement,DoubleType,true), StructField(Horsepower,LongType,true), StructField(Miles_per_Gallon,DoubleType,true), StructField(Name,StringType,true), StructField(Origin,StringType,true), StructField(Weight_in_lbs,LongType,true), StructField(Year,StringType,true))

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA") // [chevrolet chevelle malibu,18.0,8,307.0,130,3504,12.0,1970-01-01,USA]

  // create df from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  // note: DFs have schemas, rows do not

  // create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualCarsDF.printSchema() // it doesn't have column names. Each column is _1, _2, ..., _n
  manualCarsDFWithImplicits.printSchema()

  /*
    Exercise:
    1) Create a manual DF describing smartphone
      - make
      - model
      - screen dimension
      - camera megapixels

    2) Read another files from the data/ folder, e.g movies.json
      - print its schema
      - count the number of rows, call count()
   */

  val smartphones = Seq(
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "iphone X", "iOS", 13),
    ("Nokia", "3310", "THE BEST", 0),
  )

  // 1)
  val smartphonesDF = smartphones.toDF("Make", "Model", "Screen Dim", "Camera MPx")
  smartphonesDF.show()
  smartphonesDF.printSchema()

  // 2)
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(s"The Movies DF has ${moviesDF.count()} rows") // The Movies DF has 3201 rows

}
