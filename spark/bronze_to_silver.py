from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, from_unixtime

BRONZE_PATH = "/app/lake/bronze/rides"
SILVER_PATH = "/app/lake/silver/rides"
CHECKPOINT_PATH = "/app/lake/checkpoint_silver/rides"

def build_spark():
    return (SparkSession.builder
        .appName("NYC-Bronze-To-Silver")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate())

def main():
    spark = build_spark()
    
    # load as Stream (even if its batch)
    bronze_df = spark.readStream.format("delta").load(BRONZE_PATH)
    #bronze_df = spark.read.format("delta").load(BRONZE_PATH) ###
    #print(f"Count of rows in bronze layer: {bronze_df.count()}")

    # cleaning
    silver_df = bronze_df.withColumn(
        "pickup_time", 
        col("tpep_pickup_datetime").cast("timestamp")
    ).withColumn(
        "dropoff_time", 
        col("tpep_dropoff_datetime").cast("timestamp")
    ).filter(
        (col("passenger_count") > 0) & 
        (col("trip_distance") > 0.0) &
        (col("fare_amount") > 0.0)
    ).withColumn(
        "is_long_trip", when(col("trip_distance") > 10, True).otherwise(False)
    ).withColumn(
        "_silver_ingest_ts", current_timestamp()
    ).drop("tpep_pickup_datetime", "tpep_dropoff_datetime")

    # saving to silver
    query = (silver_df.writeStream
        .format("delta")
        .outputMode("append")
        # make it check every 5 sec if sth appeared, not to miss data from stream (trial and error of why srtream data missing in silver)
        .trigger(processingTime='5 seconds')
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(SILVER_PATH))
    
    #silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH) ###

    print("Silver Streaming started.")
    query.awaitTermination()

if __name__ == "__main__":
    main()