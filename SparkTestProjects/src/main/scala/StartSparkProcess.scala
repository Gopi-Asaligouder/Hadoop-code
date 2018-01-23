import org.apache.spark.sql.SparkSession

case class record(name: String, tamil: Int, Maths: Int)

object StartSparkProcess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    import spark.sqlContext.implicits._
    val df = spark.sparkContext.parallelize(Seq(record("gopi", 68, 34), record("raja", 68, 34), record("siva", 68, 34), record("swamy", 68, 34))).toDF
    df.registerTempTable("record")
    spark.sql("select record").show()

  }
}