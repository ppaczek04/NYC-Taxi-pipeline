from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    year, month, to_timestamp
)
from pyspark.sql.types import *

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "rides_stream"

BRONZE_PATH = "/app/lake/bronze/rides"
CHECKPOINT_PATH = "/app/lake/checkpoint_bronze/rides_stream"

def build_spark():
    return (
        SparkSession.builder
        .appName("NYC-Stream-To-Bronze")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN") # no need for INFO Logs

    schema = StructType([ # we set streaming_to_bronze types to default 'pandas parquet' types
        # so that they match (data types loaded by batch_to_bronze and stream_to_bronze)
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampNTZType()),
        StructField("tpep_dropoff_datetime", TimestampNTZType()),
        StructField("passenger_count", LongType()),
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
        (col("tpep_pickup_datetime"))
    )

    # NO NEED FOR THAT BECAUSE PRODUCER DID: "json.dumps(row.to_dict(), default=str)" so it was send to kafka as 
    # "2025-10-30 14:25:09" not "2025-10-30 14:25:09"
    
    # df = df.withColumn(
    #     "pickup_ts",
    #     from_unixtime(col("tpep_pickup_datetime") / 1000).cast("timestamp")
    # )

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
        .partitionBy("year", "month") #same partiiton schema as in batch_to_bronze
        .start(BRONZE_PATH)
    )

    print("Streaming started.")
    query.awaitTermination()

if __name__ == "__main__":
    main()
