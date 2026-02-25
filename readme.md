# NYC Taxi Data Pipeline

## Description
A scalable data pipeline built on the **Medallion Architecture** to process large-scale New York City taxi trip data. The system ingests raw events from Apache Kafka, manages stateful storage using Delta Lake, and performs real-time transformations to ensure data quality and analytical readiness.

## Tech Stack
* **Language:** Python (PySpark)
* **Storage:** Delta Lake (Parquet-based)
* **Processing:** Apache Spark (Standalone Cluster)
* **Streaming:** Apache Kafka
* **Infrastructure:** Docker & Docker Compose

## Architecture
The project follows the **Medallion Architecture** design pattern:

[Image of medallion architecture for data engineering with bronze, silver and gold layers]

1.  **Bronze (Raw):** Stores the raw event stream from Kafka. Includes technical metadata such as Kafka offsets, ingestion timestamps, and source file names to ensure full data lineage.
2.  **Silver (Filtered & Cleaned):** Data is refined and structured. Transformations include:
    * Casting raw fields to correct types (e.g., `Timestamp_NTZ`).
    * Filtering out invalid business records (e.g., trips with 0 passengers or negative fare amounts).
    * Initial feature engineering (e.g., calculating trip duration or `is_long_trip` flags).
3.  **Gold (Curated):** *[Planned]* Business-level aggregates and KPIs optimized for reporting and ML models.

## Flow
1.  **Ingestion:** A `producer.py` script simulates a real-time stream of taxi trip data into a Kafka topic.
2.  **Bronze Layer:** A Spark Structured Streaming job consumes the Kafka topic and writes the data into a Delta table at `/lake/bronze`.
3.  **Silver Layer:** The `bronze_to_silver.py` streaming process reads from the Bronze table, applies schema enforcement and cleaning "on-the-fly," and sinks the result into `/lake/silver`.

---

## Roadmap
* **Gold Layer