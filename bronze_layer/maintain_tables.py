import os
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# 1. Setup the Spark Session 
spark = SparkSession.builder \
    .appName("DeltaTableMaintenance") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Define the path to your raw table
table_path = "./data/raw_audit_table"

if os.path.exists(table_path):
    print(f"--- Starting Maintenance for: {table_path} ---")
    
    # 3. Load the folder as a Delta Table object
    deltaTable = DeltaTable.forPath(spark, table_path)
    
    # 4. The OPTIMIZE command
    print("Compacting small files into larger ones...")
    deltaTable.optimize().executeCompaction()
    
    # 5. The VACUUM command  
    print("Cleaning up expired data files (Vacuuming)...")
    deltaTable.vacuum(retentionHours=168) 
    
    print("--- Maintenance Complete! ---")
else:
    print(f"Error: Path {table_path} not found. Run your consumer first!")

spark.stop()