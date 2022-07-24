import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, sql}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object DataFrameExample {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .set("spark.submit.deployMode", "client")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val movieSamples = spark.read.option("header", "true").csv("src/main/resources/sampledata/movies.csv")
    val ratingSamples = spark.read.option("header", "true").csv("src/main/resources/sampledata/ratings.csv")

    val aggregateRatingMovie = movieSamples.join(ratingSamples, Seq("movieId"), "inner")

    val targetInput = aggregateRatingMovie.select("movieId", "rating")

    val ratingMovieGroupByMovie = targetInput.groupBy(col("movieId")).agg(avg("rating").alias("avg_rating"))


    val sortedOutput = ratingMovieGroupByMovie.sort(col("avg_rating").desc)

    spark.time(sortedOutput.show(4, false))
    // Time taken: 3009 ms
  }
}
