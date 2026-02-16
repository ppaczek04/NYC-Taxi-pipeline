from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit, current_timestamp, input_file_name,
    year, month, col, to_timestamp
)

RAW_PATH = "/data/yellow_tripdata_2025-*.parquet"
BRONZE_PATH = "/lake/bronze/rides"

def build_spark():
    return (
        SparkSession.builder
        .appName("NYC-Batch-To-Bronze")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(RAW_PATH)

    df = df.withColumn(
        "pickup_ts",
        to_timestamp(col("tpep_pickup_datetime"))
    )

    df = df.withColumn("year", year(col("pickup_ts"))) \
           .withColumn("month", month(col("pickup_ts")))

    df = df.withColumn("_ingest_ts", current_timestamp()) \
           .withColumn("_source", lit("batch")) \
           .withColumn("_file_name", input_file_name()) \
           .withColumn("_kafka_offset", lit(None).cast("long"))

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month")
        .save(BRONZE_PATH)
    )

    print("Batch written to Bronze.")

    spark.stop()

if __name__ == "__main__":
    main()
