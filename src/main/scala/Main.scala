import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark app")
      .getOrCreate()
  }
}

object Main extends SparkSessionWrapper {


  def main(args: Array[String]): Unit = {
    println("Hello world!")
    import spark.implicits._

    val textFile = spark.read.textFile("src/main/resources/words.txt")
    val wordCounts = textFile.flatMap(line => line.split(" "))
      .groupByKey(identity)
      .count()

    wordCounts.show()
  }
}