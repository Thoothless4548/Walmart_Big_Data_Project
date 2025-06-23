import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object Code8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeeklySalesByCPI")
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
    val trainDF = dataRDD.map(arr => (arr(0).toInt, arr(1).toInt, arr(2), arr(3).toDouble, arr(4).toBoolean))
      .toDF("Store", "Dept", "Date", "Weekly_Sales", "IsHoliday")

    // 3. Load features.csv to get CPI
    val featuresDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:/Users/ADIPAKSH/Downloads/walmart-recruiting-store-sales-forecasting_source TEAM4/features.csv")

    // 4. Join train data with CPI
    val joinedDF = trainDF.join(featuresDF.select("Store", "Date", "CPI"), Seq("Store", "Date"))

    // 5. Group by CPI and calculate total weekly sales
    val resultDF = joinedDF.groupBy("CPI")
      .agg(sum("Weekly_Sales").alias("Total_Weekly_Sales"))
      .orderBy("CPI")

    // 6. Show result
    resultDF.show()

    // 7. Write output to local file system
    resultDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output9/weekly_sales_by_cpi.csv")

    // 8. Write as Parquet and ORC
    resultDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output9/weekly_sales_by_cpi_parquet")
    resultDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output9/weekly_sales_by_cpi_orc")

    // 9. Partition by CPI
    resultDF.write.partitionBy("CPI").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output9/partitioned_by_cpi")

    // 10. SQL query
    resultDF.createOrReplaceTempView("sales_by_cpi")
    val sqlDF = spark.sql("SELECT CPI, AVG(Total_Weekly_Sales) as AvgSales FROM sales_by_cpi GROUP BY CPI ORDER BY CPI")
    sqlDF.show()

    // 11. Save SQL result
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output9/avg_sales_by_cpi.csv")

    spark.stop()
  }
}
