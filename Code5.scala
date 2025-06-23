import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object Code5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeeklySalesByStoreType")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.executor.instances", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    // 1. Load train.csv as RDD
    val rdd = spark.sparkContext.textFile("C:/Users/ADIPAKSH/Downloads/walmart-recruiting-store-sales-forecasting_source TEAM4/train.csv")

    // 2. Convert RDD to DataFrame
    val header = rdd.first()
    val dataRDD = rdd.filter(_ != header).map(_.split(","))
    import spark.implicits._
    val dfFromRDD = dataRDD.map(arr => (arr(0).toInt, arr(1).toInt, arr(2), arr(3).toDouble, arr(4).toBoolean))
      .toDF("Store", "Dept", "Date", "Weekly_Sales", "IsHoliday")

    // 3. Load stores.csv to get Store Type
    val storesDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:/Users/ADIPAKSH/Downloads/walmart-recruiting-store-sales-forecasting_source TEAM4/stores.csv")

    // 4. Join train data with store type
    val joinedDF = dfFromRDD.join(storesDF.select("Store", "Type"), "Store")

    // 5. Group by Store Type and calculate total weekly sales
    val resultDF = joinedDF.groupBy("Type")
      .agg(sum("Weekly_Sales").alias("Total_Weekly_Sales"))
      .orderBy("Type")

    // 6. Show result
    resultDF.show()

    // 7. Write output to local file system
    resultDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output6/weekly_sales_by_store_type.csv")

    // 8. Write as Parquet and ORC
    resultDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output6/weekly_sales_by_store_type_parquet")
    resultDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output6/weekly_sales_by_store_type_orc")

    // 9. Partition by Type
    resultDF.write.partitionBy("Type").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output6/partitioned_by_type")

    // 10. SQL query
    resultDF.createOrReplaceTempView("sales_by_type")
    val sqlDF = spark.sql("SELECT Type, Total_Weekly_Sales FROM sales_by_type ORDER BY Total_Weekly_Sales DESC")
    sqlDF.show()

    // 11. Save SQL result
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output6/sorted_sales_by_type.csv")

    spark.stop()
  }
}
