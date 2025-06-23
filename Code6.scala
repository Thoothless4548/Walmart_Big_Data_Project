import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object Code6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FuelPriceByYear")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.executor.instances", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    // 1. Load features.csv
    val featuresDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:/Users/ADIPAKSH/Downloads/walmart-recruiting-store-sales-forecasting_source TEAM4/features.csv")

    // 2. Extract year from Date
    val withYearDF = featuresDF.withColumn("Year", year(to_date(col("Date"), "yyyy-MM-dd")))

    // 3. Group by Year and calculate average fuel price
    val resultDF = withYearDF.groupBy("Year")
      .agg(avg("Fuel_Price").alias("Average_Fuel_Price"))
      .orderBy("Year")

    // 4. Show result
    resultDF.show()

    // 5. Write output to local file system
    resultDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output7/fuel_price_by_year.csv")

    // 6. Write as Parquet and ORC
    resultDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output7/fuel_price_by_year_parquet")
    resultDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output7/fuel_price_by_year_orc")

    // 7. Partition by Year
    resultDF.write.partitionBy("Year").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output7/partitioned_by_year_fuel")

    // 8. SQL query
    resultDF.createOrReplaceTempView("fuel_price")
    val sqlDF = spark.sql("SELECT Year, Average_Fuel_Price FROM fuel_price ORDER BY Year")
    sqlDF.show()

    // 9. Save SQL result
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output7/avg_fuel_price_by_year.csv")

    spark.stop()
  }
}
