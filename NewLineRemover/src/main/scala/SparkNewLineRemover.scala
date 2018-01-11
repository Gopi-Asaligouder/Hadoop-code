import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/*
This class is used to remove new line from source file using spark
 */

object SparkNewLineRemover {

  def main(args: Array[String]): Unit = {

    args.length match {
      case x if x < 2 => throw new Exception("Number of columns and delimiter required in the format <noOfColumns> <Delimiter>")
      case _  =>
    implicit val numOfColumn:Int = args(0).toInt
    val ss = SparkSession.builder().master("local").appName("NewLineRemovalTest").getOrCreate()

    import ss.sqlContext.implicits._

    val inputRecord: Dataset[String] = ss.read.textFile("src/main/resource/NewLineTest.txt")

    val mappedDataSet: RDD[(Int, String)] = inputRecord.rdd.flatMap(_.split(",")).filter(!_.trim.isEmpty).map((1, _))

    val pairedRdd = mappedDataSet.aggregateByKey(Nil: List[List[String]])(aggregateFunction, partitionCombiner).flatMap(record => record._2)

    pairedRdd.map(_.mkString(",")).toDF("FinalRecord").show()

    }
  }

  def aggregateFunction(initialValue: List[List[String]], incrementalRecord: String)(implicit numColumns:Int): List[List[String]] = {

    initialValue match {
      case x if x.isEmpty => List(List(incrementalRecord))
      case _ =>
        val lastElement = initialValue.last
        lastElement.size match {
          case x if x == numColumns => initialValue ++ List(List(incrementalRecord))
          case _ => initialValue.filterNot(_ == lastElement) :+ (lastElement :+ incrementalRecord)
        }
    }
  }
    def partitionCombiner(part1: List[List[String]],part2: List[List[String]])(implicit numColumns:Int): List[List[String]] ={
      part1.last.size match {
        case x if x == numColumns => part1 ++ part2
        case _ =>
          val flattedOutput = part1.flatMap(_.iterator) ++ part2.flatMap(_.iterator)
          flattedOutput.foldLeft(Nil: List[List[String]])(aggregateFunction)
      }

    }
}
