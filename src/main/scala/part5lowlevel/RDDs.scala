package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the sparkContext is the entry point for low-level APIS, including RDDs
  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers: Seq[Int] = 1 to 100
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String): List[StockValue] =
    Source.fromFile(filename)
      .getLines()
      .drop(1) // remove header
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))
//  stocksRDD.foreach(println) // StockValue(MSFT,Jan 1 2000,39.81) ...

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // remove header
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF: DataFrame = spark.read // DataFrame == DataSet[Row]
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS: Dataset[StockValue] = stocksDF.as[StockValue]
  val stocksRDD3: RDD[StockValue] = stocksDS.rdd

  // RDD -> DF
  val numbersDF: DataFrame = numbersRDD.toDF("numbers") // you lose the type info
  // DataFrame == DataSet[Row] and Row do not have type info

  // RDD -> DS
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD) // you get to keep type info

  // Transformations

  // filter (transformation), count (action)
  val msftRDD: RDD[StockValue] = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount: Long = msftRDD.count() // eager ACTION

  // distinct
  val companyNamesRDD: RDD[String] = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft: StockValue = msftRDD.min() // action
//  println(minMsft) // StockValue(MSFT,Feb 1 2009,15.81)

  // reduce
  val reduceResult: Int = numbersRDD.reduce(_ + _) // action

  // grouping
  val groupedStocksRDD: RDD[(String, Iterable[StockValue])] = stocksRDD.groupBy(_.symbol)
  //  ^^ very expensive because of shuffling


  // Partitioning

  val repartitionedStocksRDD: RDD[StockValue] = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")
  /*
  Repartitioning is EXPENSIVE. Involves Shuffling.
  Best practice: partition EARLY, then process that.
  Size of a partition 10-100 MB.
   */

  // coalesce
  val coalescedRDD: RDD[StockValue] = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
    * Exercises
    *
    * 1. Read the movies.json as an RDD.
    * 2. Show the distinct genres as an RDD.
    * 3. Select all the movies in the Drama genre with IMDB rating > 6.
    * 4. Show the average rating of movies by genre.
    */

  case class Movie(title: String, genre: String, rating: Double)

  // 1. Read the movies.json as an RDD.
  val moviesDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD: RDD[Movie] = moviesDF
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    )
    .where(
      col("genre").isNotNull and col("rating").isNotNull
    )
    .as[Movie]
    .rdd

  // 2. Show the distinct genres as an RDD.
  val genresRDD: RDD[String] = moviesRDD.map(_.genre).distinct()

  // 3. Select all the movies in the Drama genre with IMDB rating > 6.
  val goodDramasRDD: RDD[Movie] = moviesRDD.filter(movie => movie.rating > 6)

  // 4. Show the average rating of movies by genre.
  case class GenreAvgRating(genre: String, rating: Double)
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show()
  avgRatingByGenreRDD.toDF.show()
}
 /*
  reference (moviesRDD):
  +-------------------+------------------+
  |              genre|       avg(rating)|
  +-------------------+------------------+
  |          Adventure| 6.345019920318729|
  |              Drama| 6.773441734417339|
  |        Documentary| 6.997297297297298|
  |       Black Comedy|6.8187500000000005|
  |  Thriller/Suspense| 6.360944206008582|
  |            Musical|             6.448|
  |    Romantic Comedy| 5.873076923076922|
  |Concert/Performance|             6.325|
  |             Horror|5.6760765550239185|
  |            Western| 6.842857142857142|
  |             Comedy| 5.853858267716529|
  |             Action| 6.114795918367349|
  +-------------------+------------------+

  RDD (avgRatingByGenreRDD):
  +-------------------+------------------+
  |              genre|            rating|
  +-------------------+------------------+
  |Concert/Performance|             6.325|
  |            Western| 6.842857142857142|
  |            Musical|             6.448|
  |             Horror|5.6760765550239185|
  |    Romantic Comedy| 5.873076923076922|
  |             Comedy| 5.853858267716529|
  |       Black Comedy|6.8187500000000005|
  |        Documentary| 6.997297297297298|
  |          Adventure| 6.345019920318729|
  |              Drama| 6.773441734417339|
  |  Thriller/Suspense| 6.360944206008582|
  |             Action| 6.114795918367349|
  +-------------------+------------------+

 */