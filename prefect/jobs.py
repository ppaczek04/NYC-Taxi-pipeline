import docker
from prefect import flow, task, get_run_logger

# Initialize the Docker client to communicate via /var/run/docker.sock
client = docker.from_env()

@task(name="Run Spark Job via Docker")
def run_spark_script(script_path: str, memory: str = "2g", is_streaming: bool = False):
    """
    Triggers a spark-submit command inside the spark-worker container.
    Matches the manual 'docker exec' commands done manually by me/user.
    """
    logger = get_run_logger()

    container = client.containers.get("spark-worker")
    # Check if streaming script already running to avoid duplication
    if is_streaming:
        # check the process list inside the container for the script name
        exec_check = container.exec_run(f"pgrep -f {script_path}")
        if exec_check.exit_code == 0:
            logger.info(f"Stream {script_path} is already running. Skipping execution.")
            return
        
    
    logger.info(f"Task initiated: Running {script_path} with {memory} RAM allocation.")

    # Build the spark-submit command used in the terminal
    spark_command = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077", # in spark-worker console commands automatically called spark-master help
        "--conf", "spark.jars.ivy=/tmp/.ivy2", # in prefect we need to mark him as he won't be automatically found
        "--driver-memory", memory,
        "--executor-memory", memory,
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.default.parallelism=4",
        # Including both Delta and Kafka packages for all runs to prevent missing dependency errors
        "--packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        script_path
    ]

    
    if is_streaming:
        # For streaming jobs (stream_to_bronze, bronze_to_silver), we use detach=True
        # This allows Prefect to start the stream and move immediately to the next task
        container.exec_run(spark_command, detach=True)
        logger.info(f"Streaming process for {script_path} started in background.")
    else:
        # For batch jobs (Batch_to_bronze, silver_to_gold), we wait for completion
        # and capture the exit code/output to monitor success
        exit_code, output = container.exec_run(spark_command)
        
        if exit_code == 0:
            logger.info(f"Successfully completed batch job: {script_path}")
        else:
            error_msg = output.decode() if output else "No output"
            logger.error(f"Job failed ({script_path}) with code {exit_code}. Error: {error_msg}")
            raise Exception(f"Spark job failure: {script_path}")

@flow(name="NYC Taxi Medallion Pipeline")
def nyc_taxi_flow():
    """
    Main orchestration flow that executes the Medallion architecture sequence.
    """
    logger = get_run_logger()
    logger.info("Starting NYC Taxi Medallion Pipeline...")

    # 1 >> Ingest historical Parquet data into Bronze layer (everything except last month)
    # (Batch process)
    run_spark_script("/app/spark/batch_to_bronze.py", memory="2g")
    
    # 2 >> real-time Kafka ingestion into Bronze layer simulation
    # (Asynchronous stream) we load last month of data 1 records per 0.0001s (simulation of live data)
    run_spark_script("/app/spark/stream_to_bronze.py", memory="2g", is_streaming=True)
    
    # 3 >> Transform Bronze data into Silver (Cleaning/Filtering)
    # (asynchronous stream)
    run_spark_script("/app/spark/bronze_to_silver.py", memory="2g", is_streaming=True)
    
    # 4 >> Aggregate Silver data into Gold and export to Excel report
    # (Batch process)
    run_spark_script("/app/spark/silver_to_gold.py", memory="1500M")

    logger.info("Pipeline sequence initiated successfully.")

if __name__ == "__main__":
    nyc_taxi_flow.serve(
        name="nyc-taxi-pipeline-deployment",
        cron="0 * * * *" # Optional: run every hour
    )