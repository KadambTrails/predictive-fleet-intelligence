from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr

# Initialize Spark with Delta and Kafka support
spark = SparkSession.builder \
    .appName("RawLogIngestion") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," 
            "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Read the stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logistics_events") \
    .load()

# 2. Structure it 
# uuid() creates a unique log ID for every row
raw_bronze_df = raw_stream.select(
    expr("uuid()").alias("log_id"), 
    current_timestamp().alias("ingested_at"),
    col("value").cast("string").alias("raw_json_payload")
)

# 3. Write it in Real-Time to the local Delta Table
checkpoint_dir = "./checkpoints/raw_audit"
table_dir = "./data/raw_audit_table"

query = raw_bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime='10 minute') \
    .option("checkpointLocation", checkpoint_dir) \
    .start(table_dir)

print(f"Streaming started... writing raw logs to {table_dir}")
query.awaitTermination()