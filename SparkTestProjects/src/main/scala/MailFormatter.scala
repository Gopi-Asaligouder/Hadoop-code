
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

object MailFormatter {
  val logger = Logger.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("mailFormatter").master("local").getOrCreate()
    val rdd: RDD[(String, String)] = sc.sparkContext.wholeTextFiles("C:\\Users\\Indra\\mail")
    import sc.sqlContext.implicits._
    val df = rdd.flatMap(data => {
      data._2.split("FROM:").filterNot(_.trim.isEmpty).map("FROM:"+_).map(record => {
        val elements = record.split ("\n")
        val from = elements.filter (_.toUpperCase.startsWith ("FROM:") ).map (_.split (":") (1) ).mkString ("")
        val to = elements.filter (_.toUpperCase.startsWith ("TO:") ).map (_.split (":") (1) ).mkString ("")
        val subject = elements.filter (_.toUpperCase.startsWith ("SUBJECT:") ).map (_.split (":") (1) ).mkString ("")
        val body = elements.filterNot (record => (record.startsWith ("SUBJECT:") || record.startsWith ("FROM:") || record.startsWith ("TO:") ) ).mkString ("\n")
        (from, to, subject, body)
      })

    }).toDF("FROM","TO","SUBJECT","BODY")
    df.write.format("csv").mode("overwrite").save("C:\\Users\\1556793\\mail\\output")
  }
}
