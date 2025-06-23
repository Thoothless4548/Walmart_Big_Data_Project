import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object programs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeeklySalesByTempYear")
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

    // 3. Load features.csv and join
    val featuresDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:/Users/ADIPAKSH/Downloads/walmart-recruiting-store-sales-forecasting_source TEAM4/features.csv")

    val joinedDF = dfFromRDD.join(featuresDF, Seq("Store", "Date", "IsHoliday"), "left")

    // 4. Extract year and perform analysis
    val withYearDF = joinedDF.withColumn("Year", year(to_date(col("Date"), "yyyy-MM-dd")))
    val resultDF = withYearDF.groupBy("Temperature", "Year")
      .agg(sum("Weekly_Sales").alias("Total_Weekly_Sales"))
      .orderBy("Temperature", "Year")

    // 5. Write output to local file system
    resultDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output/weekly_sales_by_temp_year.csv")

    // 6. Write as Parquet and ORC
    resultDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output/weekly_sales_parquet")
    resultDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output/weekly_sales_orc")

    // 7. Cache and repartition RDD
    val cachedRDD = dataRDD.cache()
    val partitionedRDD = cachedRDD.repartition(4)

    // 8. Partition DF by Year
    resultDF.write.partitionBy("Year").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output/partitioned_by_year")

    // 9. Transformations and actions
    val filteredDF = resultDF.filter($"Total_Weekly_Sales" > 1000000)
    filteredDF.show()
    println(s"Count of filtered rows: ${filteredDF.count()}")

    // 11. SQL queries
    resultDF.createOrReplaceTempView("sales_temp")
    val sqlDF = spark.sql("SELECT Year, AVG(Total_Weekly_Sales) as AvgSales FROM sales_temp GROUP BY Year")
    sqlDF.show()

    // 12. Save SQL result as report
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output/avg_sales_report.csv")

    spark.stop()
  }
}
