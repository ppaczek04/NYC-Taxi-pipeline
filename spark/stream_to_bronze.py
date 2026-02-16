from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    year, month, to_timestamp
)
from pyspark.sql.types import *

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "rides_raw"

BRONZE_PATH = "/lake/bronze/rides"
CHECKPOINT_PATH = "/lake/checkpoint_bronze/rides_stream"

def build_spark():
    return (
        SparkSession.builder
        .appName("NYC-Stream-To-Bronze")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", StringType()),
        StructField("tpep_dropoff_datetime", StringType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("total_amount", DoubleType())
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    json_df = kafka_df.select(
        col("value").cast("string").alias("value_str"),
        col("offset")
    )

    parsed = (
        json_df
        .select(
            from_json(col("value_str"), schema).alias("data"),
            col("offset").alias("_kafka_offset")
        )
        .select("data.*", "_kafka_offset")
    )

    parsed = parsed.withColumn(
        "pickup_ts",
        to_timestamp(col("tpep_pickup_datetime"))
    )

    parsed = parsed.withColumn("year", year(col("pickup_ts"))) \
                   .withColumn("month", month(col("pickup_ts")))

    parsed = parsed.withColumn("_ingest_ts", current_timestamp()) \
                   .withColumn("_source", lit("stream")) \
                   .withColumn("_file_name", lit(None).cast("string"))

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("year", "month")
        .start(BRONZE_PATH)
    )

    print("Streaming started.")
    query.awaitTermination()

if __name__ == "__main__":
    main()
