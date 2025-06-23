import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object Code3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeeklySalesByStoreTypeMonth")
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

    // 5. Extract month from Date
    val withMonthDF = joinedDF.withColumn("Month", month(to_date(col("Date"), "yyyy-MM-dd")))

    // 6. Group by Store Type and Month
    val resultDF = withMonthDF.groupBy("Type", "Month")
      .agg(sum("Weekly_Sales").alias("Total_Weekly_Sales"))
      .orderBy("Type", "Month")

    // 7. Show result
    resultDF.show()

    // 8. Write output to local file system
    resultDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output4/weekly_sales_by_store_type_month.csv")

    // 9. Write as Parquet and ORC
    resultDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output4/weekly_sales_by_store_type_month_parquet")
    resultDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output4/weekly_sales_by_store_type_month_orc")

    // 10. Partition by Type
    resultDF.write.partitionBy("Type").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output4/partitioned_by_type")

    // 11. SQL query
    resultDF.createOrReplaceTempView("sales_by_type_month")
    val sqlDF = spark.sql("SELECT Type, Month, AVG(Total_Weekly_Sales) as AvgSales FROM sales_by_type_month GROUP BY Type, Month ORDER BY Type, Month")
    sqlDF.show()

    // 12. Save SQL result
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output4/avg_sales_by_type_month.csv")

    spark.stop()
  }
}
