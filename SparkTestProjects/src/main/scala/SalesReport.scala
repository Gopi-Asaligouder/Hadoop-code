import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

object SalesReport {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local").appName("SalesReport").getOrCreate()

    import ss.sqlContext.implicits._

    val dataConverter:String => String = input => {
      val index = input.indexOf("/")
      input.substring(0,index)
    }
    val dateToMonthCOnv = udf(dataConverter)

    val inputDF = ss.read.option("header", "true").csv("C:\\Users\\welcome\\Desktop\\Test.csv").withColumn("Product", emptyToNull($"Product")).cache()
    val highestTransDF = inputDF.withColumn("Transaction_amount", $"Transaction_amount".cast(IntegerType))
    highestTransDF.groupBy("Product").sum("Transaction_amount").orderBy($"sum(Transaction_amount)".desc).limit(1).show()
    val masterFamousCountryDf = inputDF.groupBy("Payment_Type", "Country").count().groupBy("Payment_Type", "Country")
    masterFamousCountryDf.sum("count").orderBy($"sum(count)".desc).where($"Payment_Type" === "Mastercard").limit(1).show()
    inputDF.select("Name", "Payment_Type").distinct().groupBy("Payment_Type").count().show()
    highestTransDF.withColumn("month",dateToMonthCOnv(inputDF.col("Transaction_date"))).groupBy("month").sum("Transaction_amount").show()
  }

  def emptyToNull(c: Column):Column = when(length(trim(c)) > 0, trim(c))




}
