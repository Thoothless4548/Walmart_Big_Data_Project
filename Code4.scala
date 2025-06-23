import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object Code4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MarkdownSalesByYearStore")
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

    // 3. Sum all Markdown columns
    val markdownDF = withYearDF.groupBy("Store", "Year")
      .agg(
        sum("MarkDown1").alias("Total_MarkDown1"),
        sum("MarkDown2").alias("Total_MarkDown2"),
        sum("MarkDown3").alias("Total_MarkDown3"),
        sum("MarkDown4").alias("Total_MarkDown4"),
        sum("MarkDown5").alias("Total_MarkDown5")
      )
      .orderBy("Store", "Year")

    // 4. Show result
    markdownDF.show()

    // 5. Write output to local file system
    markdownDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output5/markdown_sales_by_year_store.csv")

    // 6. Write as Parquet and ORC
    markdownDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output5/markdown_sales_by_year_store_parquet")
    markdownDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output5/markdown_sales_by_year_store_orc")

    // 7. Partition by Year
    markdownDF.write.partitionBy("Year").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output5/partitioned_by_year_markdown")

    // 8. SQL query
    markdownDF.createOrReplaceTempView("markdown_sales")
    val sqlDF = spark.sql(
      """
        |SELECT Store, Year,
        |       (Total_MarkDown1 + Total_MarkDown2 + Total_MarkDown3 + Total_MarkDown4 + Total_MarkDown5) AS Total_Markdowns
        |FROM markdown_sales
        |ORDER BY Store, Year
        |""".stripMargin)

    sqlDF.show()

    // 9. Save SQL result
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output5/total_markdowns_by_year_store.csv")

    spark.stop()
  }
}
