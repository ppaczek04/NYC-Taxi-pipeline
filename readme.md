# NYC Taxi Data Pipeline

## Description
A scalable data pipeline built on the **Medallion Architecture** to process large-scale New York City taxi trip data. The system ingests raw events from Apache Kafka, manages stateful storage using Delta Lake, and orchestrates complex streaming and batch workflows using **Prefect 3.0**.

## Tech Stack
* **Orchestration:** Prefect 3.0
* **Processing:** Apache Spark 3.5.0 (Standalone Cluster)
* **Storage:** Delta Lake 3.1.0 (ACID transactions & Schema Enforcement)
* **Streaming:** Apache Kafka
* **Language:** Python (PySpark)
* **Infrastructure:** Docker & Docker Compose

## Architecture
The project follows the **Medallion Architecture** design pattern to ensure data quality and reliability:



1.  **Bronze (Raw):** 
    * **Batch Ingestion:** Historical Parquet data is loaded via `batch_to_bronze.py`.
    * **Stream Ingestion:** Real-time event streams from Kafka are captured via `stream_to_bronze.py`.
    * Includes technical metadata (offsets, timestamps) for full lineage.
2.  **Silver (Cleaned & Augmented):** -- **Stream Processing:** `bronze_to_silver.py` reads from the Bronze Delta table.
    * **Transformations:** Schema enforcement, type casting (e.g., `Timestamp_NTZ`), and business logic filtering (removing 0-passenger or negative-fare trips).
3.  **Gold (Curated):** -- **Batch Processing:** `silver_to_gold.py` performs final business-level aggregations.
    * **Reporting:** Generates curated datasets and KPI reports in Excel format.

## Orchestration & Flow
The entire pipeline is orchestrated by **Prefect**, managing dependencies between batch and streaming jobs:



1.  **Historical Bootstrap:** Prefect triggers a Spark batch job to populate the Bronze layer with historical data.
2.  **Streaming Initialization:** Real-time ingestion starts in the background (detached mode) to handle incoming Kafka traffic.
3.  **Silver Transformation:** A secondary streaming process is launched to refine data as soon as it hits the Bronze layer.
4.  **Final Aggregation:** Once the data is ready, a final Spark batch job generates the Gold layer reports.

---

## Current Status & Roadmap
- [x] **Infrastructure:** Dockerized Spark, Kafka, and Prefect environment.
- [*] **Bronze Layer:** Unified ingestion for both Batch and Stream. *(debugging)*
- [x] **Silver Layer:** Real-time cleaning and schema evolution.
- [x] **Orchestration:** Automated flow sequence with Docker-based job submission.
- [ ] **Gold Layer (Optimization):** Refine complex window aggregations and KPI calculations. *(Planned)*
- [ ] **Data Quality Checks:** Implementation of Great Expectations or Delta Expectations for automated validation. *(Planned)*