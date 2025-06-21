import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Requirement8{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    //group by item type and count the number of orders
    val itemOrdersDF=salesDF.groupBy("Item Type").agg(count("Order ID").as("Number_of_orders")).persist()
    itemOrdersDF.show()

    itemOrdersDF.coalesce(1).write.partitionBy("Item Type").parquet("C:/Users/gomall/Desktop/requirement8.csv")
    spark.stop()


  }

}