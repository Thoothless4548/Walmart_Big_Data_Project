import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object Code7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeeklySalesByYearMonthDate")
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

    // 3. Extract Year, Month, and Date
    val withDateDF = dfFromRDD.withColumn("ParsedDate", to_date(col("Date"), "yyyy-MM-dd"))
      .withColumn("Year", year(col("ParsedDate")))
      .withColumn("Month", month(col("ParsedDate")))
      .withColumn("Day", dayofmonth(col("ParsedDate")))

    // 4. Group by Year, Month, and Date
    val resultDF = withDateDF.groupBy("Year", "Month", "Date")
      .agg(sum("Weekly_Sales").alias("Total_Weekly_Sales"))
      .orderBy("Year", "Month", "Date")

    // 5. Show result
    resultDF.show()

    // 6. Write output to local file system
    resultDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output8/weekly_sales_by_year_month_date.csv")

    // 7. Write as Parquet and ORC
    resultDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output8/weekly_sales_by_year_month_date_parquet")
    resultDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output8/weekly_sales_by_year_month_date_orc")

    // 8. Partition by Year and Month
    resultDF.write.partitionBy("Year", "Month").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output8/partitioned_by_year_month")

    // 9. SQL query
    resultDF.createOrReplaceTempView("sales_by_date")
    val sqlDF = spark.sql("SELECT Year, Month, AVG(Total_Weekly_Sales) as AvgSales FROM sales_by_date GROUP BY Year, Month ORDER BY Year, Month")
    sqlDF.show()

    // 10. Save SQL result
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output8/avg_sales_by_year_month.csv")

    spark.stop()
  }
}
