Ingestion Layer: Apache Kafka handles the ingestion of real-time geospatial coordinates, truck speeds, and engine temperature streams.

Stream Processing: Apache PySpark reads continuously from Kafka topics, structured window partitions, and flushes historical immutable events into a Bronze Parquet Lakehouse.

Storage & Transformation (Silver): dbt (Data Build Tool) coupled with DuckDB reads raw Parquet files, establishes schemas, cleans type casts, and builds optimized analytical tables inside the Silver layer.

Data Quality Gates: Automated testing constraints implemented natively via dbt test, validating null values, ranges, and multi-point custom sensor anomalies before advancing downstream.

Analytical Serving Layer (Gold): A standalone Python pipeline extracts validated matrices from DuckDB and executes high-performance bulk upserts into a ClickHouse OLAP columnar warehouse.

Orchestration Control Room: Apache Airflow deployed inside Docker Desktop containers acts as the master traffic cop, orchestrating the batch transformations, data quality tests, and data loading gates via sequential Directed Acyclic Graphs (DAGs).