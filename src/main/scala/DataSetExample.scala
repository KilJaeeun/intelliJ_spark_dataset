import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.expressions.scalalang._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Encoder, Encoders}

case class Movie(
                  title: String,
                  genres: String,
                  movieId: String
                )

case class Review(
                   movieId: String,
                   userId: String,
                   rating: Double,
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
    val ratingSamples = spark.read.option("header", "true").csv("src/main/resources/sampledata/ratings.csv").withColumn("rating", Symbol("rating").cast(DoubleType)).as[Review]

    val aggregateRatingMovie = movieSamples.join(ratingSamples, Seq("movieId"), "inner")
    movieSamples.join(ratingSamples, Seq("movieId"), "inner").explain()
    /*
    == Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [named_struct(movieId, movieId#17, title, title#18, genres, genres#19) AS _1#123, named_struct(userId, userId#47, movieId, movieId#48, rating, rating#49, timestamp, timestamp#50) AS _2#124]
   +- BroadcastHashJoin [movieId#17], [movieId#48], Inner, BuildLeft, false
      :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [id=#168]
      :  +- Filter isnotnull(movieId#17)
      :     +- FileScan csv [movieId#17,title#18,genres#19] Batched: false, DataFilters: [isnotnull(movieId#17)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/giljaeeun/Documents/work_dir/naver/20220724/dataset_proj/s..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:string,title:string,genres:string>
      +- Filter isnotnull(movieId#48)
         +- FileScan csv [userId#47,movieId#48,rating#49,timestamp#50] Batched: false, DataFilters: [isnotnull(movieId#48)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/giljaeeun/Documents/work_dir/naver/20220724/dataset_proj/s..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<userId:string,movieId:string,rating:string,timestamp:string>


     */


    // aggregateRatingMovie.show(3, false)
    val aggregateRatingMovie2 = movieSamples.joinWith(ratingSamples, movieSamples.col("movieId") === movieSamples.col("movieId"))
    movieSamples.joinWith(ratingSamples, movieSamples.col("movieId") === movieSamples.col("movieId")).explain()
    /*
    == Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [named_struct(movieId, movieId#17, title, title#18, genres, genres#19) AS _1#123, named_struct(userId, userId#47, movieId, movieId#48, rating, rating#49, timestamp, timestamp#50) AS _2#124]
   +- BroadcastHashJoin [movieId#17], [movieId#48], Inner, BuildLeft, false
      :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [id=#168]
      :  +- Filter isnotnull(movieId#17)
      :     +- FileScan csv [movieId#17,title#18,genres#19] Batched: false, DataFilters: [isnotnull(movieId#17)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/giljaeeun/Documents/work_dir/naver/20220724/dataset_proj/s..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<movieId:string,title:string,genres:string>
      +- Filter isnotnull(movieId#48)
         +- FileScan csv [userId#47,movieId#48,rating#49,timestamp#50] Batched: false, DataFilters: [isnotnull(movieId#48)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/giljaeeun/Documents/work_dir/naver/20220724/dataset_proj/s..., PartitionFilters: [], PushedFilters: [IsNotNull(movieId)], ReadSchema: struct<userId:string,movieId:string,rating:string,timestamp:string>

     */
    //   aggregateRatingMovie2.show(3, false)
    /*


    +-------------------------------------------------------------------------------------------------------------+------------------------+
    |_1                                                                                                           |_2                      |
    +-------------------------------------------------------------------------------------------------------------+------------------------+
    |{2, Jumanji (1995), Adventure|Children|Fantasy}                                                              |{1, 2, 3.5, 1112486027} |
    |{29, City of Lost Children, The (Cité des enfants perdus, La) (1995), Adventure|Drama|Fantasy|Mystery|Sci-Fi}|{1, 29, 3.5, 1112484676}|
    |{32, Twelve Monkeys (a.k.a. 12 Monkeys) (1995), Mystery|Sci-Fi|Thriller}                                     |{1, 32, 3.5, 1112484819}|
    +-------------------------------------------------------------------------------------------------------------+------------------------+
     */


    val targetInput = aggregateRatingMovie.select("movieId", "rating")
    val ratingMovieGroupByMovie = targetInput.groupBy(col("movieId")).agg(avg("rating").alias("avg_rating"))
    targetInput.groupBy(col("movieId")).agg(avg("rating").alias("avg_rating")).explain()
    val ratingMovieGroupByMovie2 = aggregateRatingMovie2.groupByKey(_._1.movieId).agg(typed.avg(_._2.rating))
    aggregateRatingMovie2.groupByKey(_._1.movieId).agg(typed.avg(_._2.rating)).explain()
  }

}
