import org.apache.spark.{SparkConf, sql}

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
    movieSamples.join(ratingSamples, Seq("movieId"), "inner").explain()
/*
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [movieId#17, title#18, genres#19, userId#40, rating#42, timestamp#43]
   +- BroadcastHashJoin [movieId#17], [movieId#41], Inner, BuildLeft, false
      :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [id=#64]
      :  +- Filter isnotnull(movieId#17)
      :     +- FileScan csv [movieId#17,title#18,genres#19] Batched: false, DataFilters: [isnotnull(movieId#17)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/giljaeeun/Documents/work_dir/naver/20220724/dataset_proj/s..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:string,title:string,genres:string>
      +- Filter isnotnull(movieId#41)
         +- FileScan csv [userId#40,movieId#41,rating#42,timestamp#43] Batched: false, DataFilters: [isnotnull(movieId#41)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/giljaeeun/Documents/work_dir/naver/20220724/dataset_proj/s..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<userId:string,movieId:string,rating:string,timestamp:string>

 */


    val targetInput = aggregateRatingMovie.select("movieId", "rating")
    val ratingMovieGroupByMovie = targetInput.groupBy(col("movieId")).agg(avg("rating").alias("avg_rating"))


    val sortedOutput = ratingMovieGroupByMovie.sort(col("avg_rating").desc)

    spark.time(sortedOutput.show(4, false))
    // Time taken: 3009 ms
  }
}
