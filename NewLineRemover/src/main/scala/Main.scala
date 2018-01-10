import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .master("local")
      .appName("NewLineRemovalTest")
      .getOrCreate()
    import ss.sqlContext.implicits._
    val inputRecord: Dataset[String] = ss.read
      .textFile("C:\\Users\\welcome\\Desktop\\NewLineTest.txt")
      .repartition(1)

    val mappedDataSet: RDD[(Int, String)] =
      inputRecord.rdd.flatMap(_.split(",")).filter(!_.trim.isEmpty).map((1, _))

    mappedDataSet
      .aggregateByKey(Nil: List[List[String]])(
        lineRemovalFunction,
        (partition1, partition2) => partition1 ++ partition2)
      .flatMap(record => record._2)
      .map(_.mkString(","))
      .toDF("FinalRecord")
      .show()

  }

  def lineRemovalFunction(initialValue: List[List[String]],
                          incrementalRecord: String): List[List[String]] = {
    println(s"a  $initialValue b  $incrementalRecord")
    if (initialValue.isEmpty) {
      List(List(incrementalRecord))
    } else {

      val lastElement = initialValue.last
      val lastSize = lastElement.size
      if (lastSize == 3) {
        initialValue ++ List(List(incrementalRecord))
      } else {
        initialValue.filterNot(_ == lastElement) :+ (lastElement :+ incrementalRecord)
      }

    }

  }
}
