package part2dataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandDF = guitaristsDF
    .join(bandsDF, joinCondition, "inner") // inner is the default joinType
    .select("*")

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = everything in the left DF for which there is a row in the DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = everything in the left DF for which there is NO row in the DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // things to bear in mind
  //  guitaristsBandDF.select("id", "band") // this crashes

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex type
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /*
    Exercises
    1 - show all the employees and their max salary
    2 - show all the employees who were never managers
    3 - find the job titles of the best paid 10 employees in the company (latest)
   */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val username = "docker"
  val password = "docker"

  def DfFromPostgres(tableName: String): sql.DataFrame = {
    val optionsMap = Map(
      "driver" -> driver,
      "url" -> url,
      "user" -> username,
      "password" -> password,
      "dbtable" -> s"public.$tableName",
    )
    spark.read
      .format("jdbc")
      .options(optionsMap)
      .load()
  }

  val employeesDF = DfFromPostgres("employees")
  val salariesDF = DfFromPostgres("salaries")
  val managersDF = DfFromPostgres("dept_manager")
  val titlesDF = DfFromPostgres("titles")

  // 1 - show all the employees and their max salary
  val maxSalaryByEmpNoDF = salariesDF
    .groupBy(col("emp_no"))
    .agg(max("salary").as("maxSalary"))

  val employeesMaxSalaryDF = employeesDF
    .join(
      maxSalaryByEmpNoDF,
      "emp_no")
//    .select(
//      col("first_name"),
//      col("last_name"),
//      col("maxSalary"),
//    )
  employeesMaxSalaryDF.show()

  // 2 - show all the employees who were never managers
  val employeesNeverManagersDF = employeesDF
    .join(
      managersDF,
      employeesDF.col("emp_no") === managersDF.col("emp_no"),
      "left_anti")
  employeesNeverManagersDF.show()

  //3 - find the job titles of the best paid 10 employees in the company (latest)
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesMaxSalaryDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidTitlesDF = bestPaidEmployeesDF
    .join(mostRecentJobTitlesDF, bestPaidEmployeesDF.col("emp_no") === mostRecentJobTitlesDF.col("emp_no"))
    .orderBy(col("maxSalary").desc)

  bestPaidTitlesDF.show()

}

