from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, sum, count, month, year

def build_spark():
    return (SparkSession.builder 
        .appName("NYC-Silver-To-Gold") 
        .master("spark://spark-master:7077") 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .getOrCreate())

def main():
    spark = build_spark()

    # Load Silver data (Batch mode)
    silver_df = spark.read.format("delta").load("/app/lake/silver/rides")

    # Business Logic: Hourly Statistics
    gold_hourly_stats = silver_df.groupBy(
        hour(col("pickup_time")).alias("pickup_hour")
    ).agg(
        count("*").alias("total_trips"),
        avg("fare_amount").alias("avg_fare"),
        sum("tip_amount").alias("total_tips")
    ).orderBy("pickup_hour")

    # Save to Gold
    gold_hourly_stats.write.format("delta") \
        .mode("overwrite") \
        .save("/app/lake/gold/hourly_stats")

    # Export to Excel for stakeholders
    pandas_df = gold_hourly_stats.toPandas()
    pandas_df.to_excel("/app/lake/gold/hourly_report.xlsx", index=False)


    gold_monthly_stats = silver_df.groupBy(
        year(col("pickup_time")).alias("year"),
        month(col("pickup_time")).alias("month")
    ).agg(
        count("*").alias("trip_count")
    ).orderBy("year", "month")
    gold_monthly_stats.write.format("delta") \
        .mode("overwrite") \
        .save("/app/lake/gold/monthly_stats")
    pandas_df = gold_monthly_stats.toPandas()
    pandas_df.to_excel("/app/lake/gold/monthly_report.xlsx", index=False)
    
    
    print("Gold Delta table and Excel report generated.")
    spark.stop()

if __name__ == "__main__":
    main()