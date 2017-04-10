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
  }

  def runProgram(): Unit  = {

    /**
      * Note that I can the below code, instead of the above. As long as
      * I comment comment out countrySummaryTyped and use the df passed
      * in instead of the typed, generated ds
      */

    val ds = spark
      .read
      .option("header","true")
      .format("csv")
      .schema(dfSchema(List("name","country","rating")))
      .load(fsPath("/philosophers/philosophers.csv"))
      .as[PhilosopherRow]


    println("Rating > 5 works with no schema and can be applied to string col")
    ds.printSchema()
    ds.filter($"rating" > 5).show()

    val avgPhilRatingByCountry = ratingsByCountry(ds.select($"country", $"rating").as[CountryRow])

    avgPhilRatingByCountry.show()

    println("Desc")
    val descPhilByCountry =
      ds
        .groupBy('country)
        .agg(
          min('rating).as("minRating"),
          max('rating).as("maxRating"),
          max('rating).minus(min('rating)).as("range")
        ).as[Descriptive]

    descPhilByCountry.show()

    //Becasue of as[PhilosopherRow] I can access the filed .country nice!
    descPhilByCountry.collect().head.country


    descPhilByCountry.select(($"minRating" + $"maxRating").as("added")).show()


    println("Rating > 5")
    ds.filter($"rating" > 5).show()

    println("Rating > 5 again")
    ds.filter(ds("rating") > 5).show()

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
        StructField(columnNames.last, DoubleType, nullable = false)

    )
  }

  def ratingsByCountry(ds: Dataset[CountryRow]): Dataset[CountryRow] = {

    /**DataFrame to DataSet
      *
      * Remember to import implicits outside scope of object of case class
      * Remember a ds is types so you must gives types to results with .as[type]
      */

    ds
      .groupBy('country)
      .agg(
        avg('rating).as("rating")
      ).as[CountryRow]
  }

  def bestPhilosopherByCountry(ds: Dataset[CountryRow] ): Dataset[CountryRow] = {

    /**DataFrame to DataSet
      *
      * Remember to import implicits outside scope of object of case class
      * Remember a ds is types so you must gives types to results with .as[type]
      */


    ds
      .groupBy('country)
      .agg(
        max('rating).as("rating")
      ).as[CountryRow]
  }

  def descPhilosopherByCountry(ds: Dataset[Descriptive]): Dataset[Descriptive] = {

    /**DataFrame to DataSet
      *
      * Remember to import implicits outside scope of object of case class
      * Remember a ds is types so you must gives types to results with .as[type]
      */

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