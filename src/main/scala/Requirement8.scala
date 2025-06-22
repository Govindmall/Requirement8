import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Requirement8{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    //group by item type and count the number of orders
    val salesDF1=salesDF.coalesce(1)
    val itemOrdersDF=salesDF1.groupBy("Item Type").agg(count("Order ID").as("Number_of_orders")).persist()
    itemOrdersDF.show()
    itemOrdersDF.write.partitionBy("Item Type").parquet("C:/Users/gomall/Desktop/requirement8.csv")
    spark.stop()
  }
}