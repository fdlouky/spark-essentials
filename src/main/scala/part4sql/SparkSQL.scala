package part4sql

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQL extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    // only for Spark 2.4 users:
//    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF
    .select(col("Name"))
    .where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF: DataFrame = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF: DataFrame = spark.sql("show databases")

  // transfer tables from a DB to Spark tables (in this case from PostgreSQL)
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false): Unit = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse)
    tableDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  // read DF from loaded Spark tables
  val employeesDF2 = spark.read.table("employees")

  /**
    * Exercises
    *
    * 1. Read the movies DF and store it as Spark table in the rtjvm database.
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    */

  // 1 Read the movies DF and store it as Spark table in the rtjvm database.
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("movies")

  // 2 Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin
  ) // 152

  // 3 Show the average salaries for the employees hired in between those dates, grouped by department.
  spark.sql(
    """
      |select e.first_name, e.last_name, avg(s.salary) as avg_salary, d.dept_name
      |from employees e, salaries s, dept_emp de, departments d
      |where true
      |       and hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |       and e.emp_no = s.emp_no
      |       and e.emp_no = de.emp_no
      |       and de.dept_no = d.dept_no
      |group by e.first_name, e.last_name, d.dept_name
      |""".stripMargin
  ).createOrReplaceTempView("avg_salary_per_emp_and_dep")

  // 4 Show the name of the best-paying department for employees hired in between those dates.
  spark.sql(
    """
      |select dept_name, avg_salary
      |from avg_salary_per_emp_and_dep
      |order by avg_salary desc
      |limit 1
      |""".stripMargin
  ) // Sales 103035.25
}
