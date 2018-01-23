import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StudentScenario {

  def main(args: Array[String]): Unit = {

    val input = "src\\main\\resources\\studentData.txt"

    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

    import spark.implicits._
    val inputDF = spark.read.option("header", "true").csv(input).cache()

    val totalAgg = inputDF.groupBy("subject","name").agg(count("*").alias("count"))

    val stuAgg = inputDF.where("result = 'R'").groupBy("subject", "name").agg(count("*").alias("stu_count"))

    val percentageDf = stuAgg.join(totalAgg, Seq("subject","name"), "left").withColumn("percentage", calPercentile($"count", $"stu_count"))

//    percentageDf.show()

    val avg = stuAgg.join(totalAgg, Seq("subject","name"), "left").withColumn("Avg",calAverage($"count", $"stu_count"))
//    avg.show()

    inputDF.filter(row => {
     val record =  row.get(3) match{ case x if x != null => x.toString
     case _ => null}
      (!Seq("w","","W",null).contains(record.trim))
    } ).show
  }

  def calPercentile = udf((column: Int, column2: Int) => {
    val output = (column2.toDouble/column) * 100
    output
  })

  def calAverage = udf((column: Int, column2: Int) => {
    val output = (column2.toDouble/column)
    output
  })
}
