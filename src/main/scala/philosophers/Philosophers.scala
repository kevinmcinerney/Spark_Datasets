/**
  * Created by kevin on 08/04/17.
  */
package philosophers

import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._





/** Main class */
object Philosophers {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._


  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local[*]")
      .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    runProgram()

    /*val rdd = spark.read.option("header","true").format("csv").load(fsPath("/philosophers/philosophers.csv"))


    val dff = rdd.toDF("name", "country", "rating")

    dff.show()

    dff.printSchema()

    dff.createOrReplaceTempView("philosophers")

    val html = scala.io.Source.fromURL("https://www.oddschecker.com/horse-racing/naas").mkString("")
    html.foreach(l => print(l))
    //val rdds = spark.sparkContext.parallelize(list)
    //rdds.toDF().first().get(0)*/

  }

  def runProgram(): Unit  = {

    val (headings, df) = read("/philosophers/philosophers.csv")

    /**
      * Note that I can the below code, instead of the above. As long as
      * I comment comment out countrySummaryTyped and use the df passed
      * in instead of the typed, generated ds
      */
    import spark.implicits._


    println("Rating > 5 works with no schema and can be applied to string col")
    df.printSchema()
    df.filter($"rating" > 5).show()



    val avgPhilRatingByCountry = ratingsByCountry(df)

    avgPhilRatingByCountry.show()

    val philDs = philosopherSummaryTyped(df)

    philDs.show()

    val descPhilByCountry = descPhilosopherByCountry(df)

    descPhilByCountry.show()

    descPhilByCountry.select(($"minRating" + $"maxRating").as("added")).show()


    println("Rating > 5")
    df.filter($"rating" > 5).show()

    println("Rating > 5 again")
    df.filter(df("rating") > 5).show()

   // df.agg()


  }


  /** @return The read DataFrame along with its column names. */
  def read(resource: String): (List[String], DataFrame) = {

    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]

    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    (headerColumns, dataFrame)
  }



  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String = {
    val path = Paths.get(getClass.getResource(resource).toURI).toString
    //println(path)
    path
  }




  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNames Column names of the DataFrame
    */
  def dfSchema(columnNames: List[String]): StructType = {

    StructType(
      columnNames.init.map(col => {
        StructField(col, StringType, nullable = false)
      }) :+
       StructField(columnNames.last, IntegerType, nullable = false)

    )

  }


  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = {

    Row((line.init.map(_.toString) :+ line.last.toInt): _*)

    //or
    //Row.fromSeq(line.init.map(_.toString) :+ line.last.toInt)
  }



  def philosopherSummaryTyped(philosopherSummaryDf: DataFrame): Dataset[PhilosopherRow] =
    philosopherSummaryDf.as[PhilosopherRow]


  def countrySummaryTyped(philosopherSummaryDf: DataFrame): Dataset[CountryRow] =
    philosopherSummaryDf.select("country", "rating").as[CountryRow]

  def descriptiveTyped(philosopherSummaryDf: DataFrame): Dataset[Descriptive] =
    philosopherSummaryDf.select("country", "rating")
      .withColumn("minRating", 'rating)
      .withColumn("maxRating", 'rating)
      .withColumn("range", 'rating)
      .as[Descriptive]



  def ratingsByCountry(df: DataFrame): Dataset[CountryRow] = {

    /**DataFrame to DataSet
      *
      * Remember to import implicits outside scope of object of case class
      * Remember a ds is types so you must gives types to results with .as[type]
      */
    val ds = philosopherSummaryTyped(df)

    ds
      .groupBy('country)
      .agg(
        avg('rating).as("rating")
      ).as[CountryRow]
  }

  def bestPhilosopherByCountry(df: DataFrame ): Dataset[CountryRow] = {

    /**DataFrame to DataSet
      *
      * Remember to import implicits outside scope of object of case class
      * Remember a ds is types so you must gives types to results with .as[type]
      */

    val ds = philosopherSummaryTyped(df)

    ds
      .groupBy('country)
      .agg(
        max('rating).as("rating")
      ).as[CountryRow]
  }

  def descPhilosopherByCountry(df: DataFrame): Dataset[Descriptive] = {

    /**DataFrame to DataSet
      *
      * Remember to import implicits outside scope of object of case class
      * Remember a ds is types so you must gives types to results with .as[type]
      */

    val ds = descriptiveTyped(df)

    ds
      .groupBy('country)
      .agg(
        min('rating).as("minRating"),
        max('rating).as("maxRating"),
        max('rating).minus(min('rating)).as("range")
      ).as[Descriptive]
  }


}


/** Philosopher Class **/
case class PhilosopherRow(name: String, country: String, rating: Double)

/** Philosopher Class **/
case class CountryRow(country: String, rating: Double)

/** Descriptive Statistics **/
case class Descriptive(country:String, minRating: Double, maxRating: Double, range: Double)