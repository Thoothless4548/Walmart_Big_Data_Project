import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.io.File

object Code9 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WeeklySalesByDepartment")
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

    // 3. Group by Department and calculate total weekly sales
    val resultDF = trainDF.groupBy("Dept")
      .agg(sum("Weekly_Sales").alias("Total_Weekly_Sales"))
      .orderBy("Dept")

    // 4. Show result
    resultDF.show()

    // 5. Write output to local file system
    resultDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output10/weekly_sales_by_department.csv")

    // 6. Write as Parquet and ORC
    resultDF.write.mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output10/weekly_sales_by_department_parquet")
    resultDF.write.mode("overwrite").orc("C:/Users/ADIPAKSH/Desktop/output10/weekly_sales_by_department_orc")

    // 7. Partition by Department
    resultDF.write.partitionBy("Dept").mode("overwrite").parquet("C:/Users/ADIPAKSH/Desktop/output10/partitioned_by_department")

    // 8. SQL query
    resultDF.createOrReplaceTempView("sales_by_department")
    val sqlDF = spark.sql("SELECT Dept, Total_Weekly_Sales FROM sales_by_department ORDER BY Total_Weekly_Sales DESC")
    sqlDF.show()

    // 9. Save SQL result
    sqlDF.write.mode("overwrite").csv("C:/Users/ADIPAKSH/Desktop/output10/sorted_sales_by_department.csv")

    spark.stop()
  }
}
