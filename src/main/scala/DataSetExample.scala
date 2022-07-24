import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, sql}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders}

case class Movie(
                  title: String,
                  genres: String,
                  movieId: String
                )
case class Review(
                   movieId: String,
                   userId: String,
                   rating: String,
                   timestamp: String
                 )
object DataSetExample {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .set("spark.submit.deployMode", "client")
    val spark = SparkSession.builder.config(conf).getOrCreate

   import spark.implicits._

    val movieSamples = spark.read.option("header", "true").csv("src/main/resources/sampledata/movies.csv").as[Movie]
    val ratingSamples = spark.read.option("header", "true").csv("src/main/resources/sampledata/ratings.csv").as[Review ]

movieSamples.show(3,false)


  }

}
