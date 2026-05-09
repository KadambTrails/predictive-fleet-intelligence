import os
import duckdb
import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# Fetch variables from the environment
CH_HOST = os.getenv('CH_HOST')
CH_PORT = os.getenv('CH_PORT')
CH_USER = os.getenv('CH_USER')
CH_PASS = os.getenv('CH_PASSWORD')
DB_PATH = os.getenv('DUCKDB_PATH')

# 1. Connect to DuckDB and pull the Silver View
duck_conn = duckdb.connect(DB_PATH)
print(f"Pulling data from {DB_PATH}")
df = duck_conn.execute("SELECT * FROM silver_truck_data").df()

# 2. Apply the Logic from your SQL Query
# Numerical Features & Calculations
df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
df['engine_stress_index'] = (df['engine_temp'] * df['cargo_weight_kg']) / 1000

# Weather Encoding (CASE WHEN weather = ...)
df['is_weather_clear'] = df['weather'].apply(lambda x: 1 if x == 'clear' else 0)
df['is_weather_hail'] = df['weather'].apply(lambda x: 1 if x == 'hailstorm' else 0)
df['is_weather_rain'] = df['weather'].apply(lambda x: 1 if x == 'heavy rain' else 0)

# Route Encoding (CASE WHEN city = ...)
df['from_austin'] = df['source_city'].apply(lambda x: 1 if x == 'Austin' else 0)
df['to_charlotte'] = df['dest_city'].apply(lambda x: 1 if x == 'Charlotte' else 0)

# Target Labels (Labels for our 4 Algos)
df['label_maintenance_required'] = df['thermal_status'].apply(lambda x: 1 if x == 'CRITICAL' else 0)
df['label_is_anomaly'] = df['hazardous_driving_flag'].astype(int)

# 3. Connect to ClickHouse (WSL)
# REMINDER: Use your actual WSL IP here (hostname -I)
ch_client = clickhouse_connect.get_client(
        host=CH_HOST, 
        port=int(CH_PORT), 
        username=CH_USER, 
        password=CH_PASS
    )

# 4. Prepare the Gold Table in ClickHouse
ch_client.command('CREATE DATABASE IF NOT EXISTS truck_logistics_gold')
ch_client.command('DROP TABLE IF EXISTS truck_logistics_gold.gold_truck_ml_features')

# We define the schema to match your SELECT statement exactly
ch_client.command('''
    CREATE TABLE truck_logistics_gold.gold_truck_ml_features (
        truck_id String,
        event_timestamp DateTime,
        engine_temp Float64,
        avg_speed_mph Float64,
        fuel_percent Float64,
        cargo_weight_kg Float64,
        engine_stress_index Float64,
        is_weather_clear UInt8,
        is_weather_hail UInt8,
        is_weather_rain UInt8,
        from_austin UInt8,
        to_charlotte UInt8,
        label_maintenance_required UInt8,
        label_is_anomaly UInt8
    ) ENGINE = MergeTree() ORDER BY event_timestamp
''')

# 5. Insert the Data
ch_client.insert_df('truck_logistics_gold.gold_truck_ml_features', df)

print(f"Gold Layer Built! {len(df)} feature vectors pushed to ClickHouse.")