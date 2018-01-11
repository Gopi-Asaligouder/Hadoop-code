import org.apache.spark.sql.SparkSession

object StartSparkProcess {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().master("local").appName("test").getOrCreate()
    val rdd = sc.sparkContext.wholeTextFiles("C:\\Users\\welcome\\Documents\\Test")
    rdd.foreach(println(_))
  }
}
