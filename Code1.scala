import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object Code1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeeklySalesByStoreHoliday")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.executor.instances", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    // 1. Load RDD
    val rdd = spark.sparkContext.textFile("C:/Users/ADIPAKSH/Downloads/walmart-recruiting-store-sales-forecasting_source TEAM4/train.csv")

    // 2. Convert RDD to DataFrame
    val header = rdd.first()
    val dataRDD = rdd.filter(_ != header).map(_.split(","))
    import spark.implicits._
    val dfFromRDD = dataRDD.map(arr => (arr(0).toInt, arr(1).toInt, arr(2), arr(3).toDouble, arr(4).toBoolean))
      .toDF("Store", "Dept", "Date", "Weekly_Sales", "IsHoliday")

    // 3. Group by Store and IsHoliday
    val resultDF = dfFromRDD.groupBy("Store", "IsHoliday")
      .agg(sum("Weekly_Sales").alias("Total_Weekly_Sales"))
      .orderBy("Store", "IsHoliday")

    // 4. Show result
    resultDF.show()

    // 5. Write output to local file system
    resultDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output1/weekly_sales_by_store_holiday.csv")

    // 6. Write as Parquet and ORC
    resultDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output1/weekly_sales_by_store_holiday_parquet")
    resultDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output1/weekly_sales_by_store_holiday_orc")

    // 7. Cache and repartition RDD
    val cachedRDD = dataRDD.cache()
    val partitionedRDD = cachedRDD.repartition(4)

    // 8. Partition DF by IsHoliday
    resultDF.write.partitionBy("IsHoliday").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output1/partitioned_by_holiday")

    // 9. Transformations and actions
    val filteredDF = resultDF.filter($"Total_Weekly_Sales" > 10000000)
    filteredDF.show()
    println(s"Count of filtered rows: ${filteredDF.count()}")

    // 10. SQL queries
    resultDF.createOrReplaceTempView("sales_by_store_holiday")
    val sqlDF = spark.sql("SELECT IsHoliday, AVG(Total_Weekly_Sales) as AvgSales FROM sales_by_store_holiday GROUP BY IsHoliday")
    sqlDF.show()

    // 11. Save SQL result as report
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output1/avg_sales_by_holiday.csv")

    spark.stop()
  }
}
