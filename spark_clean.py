import time
import os
import sys
import glob

py_bin = "/home/vboxuser/.pyenv/versions/3.10.14/bin/python"
os.environ["PYSPARK_PYTHON"] = py_bin
os.environ["PYSPARK_DRIVER_PYTHON"] = py_bin

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType, LongType, TimestampType

start_wall_clock = time.time()

# 1. Connect to Standalone Spark Master
spark = SparkSession.builder \
    .appName("NYCTaxiSparkBenchmark") \
    .master("spark://127.0.0.1:7077") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.pyspark.python", py_bin) \
    .config("spark.pyspark.driver.python", py_bin) \
    .getOrCreate()

data_dir = os.path.expanduser("~/da3408_a3_scale/data")

# 2. Ingestion with explicit per-file casting to handle NYC TLC schema drift
print("[SPARK] Ingesting and reconciling Parquet files...")
parquet_files = sorted(glob.glob(f"{data_dir}/raw/yellow_tripdata_*.parquet"))

dfs = []
for f in parquet_files:
    df_part = spark.read.parquet(f).select(
        col("tpep_pickup_datetime").cast(TimestampType()),
        col("tpep_dropoff_datetime").cast(TimestampType()),
        col("PULocationID").cast(LongType()),
        col("trip_distance").cast(DoubleType())
    )
    dfs.append(df_part)

trips_df = dfs[0]
for next_df in dfs[1:]:
    trips_df = trips_df.unionByName(next_df)

zones_df = spark.read.option("header", "true").csv(f"{data_dir}/raw/taxi_zone_lookup.csv") \
    .select(col("LocationID").cast(LongType()), col("Borough"), col("Zone"), col("service_zone"))

# 3. Cleansing
print("[SPARK] Cleansing data (null drops & filtering)...")
clean_df = trips_df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "trip_distance"]) \
    .filter(col("trip_distance") > 0)

# 4. Heavy Join with Taxi Zones
print("[SPARK] Performing Heavy Join with Taxi Zones...")
joined_df = clean_df.join(zones_df, clean_df["PULocationID"] == zones_df["LocationID"], "inner")

# 5. Custom Python UDF (Average Speed)
def calculate_speed_mph(distance, pickup_dt, dropoff_dt):
    if not pickup_dt or not dropoff_dt or distance is None:
        return 0.0
    duration_hours = (dropoff_dt - pickup_dt).total_seconds() / 3600.0
    if duration_hours <= 0.0 or duration_hours > 24.0:
        return 0.0
    return float(distance / duration_hours)

speed_udf = udf(calculate_speed_mph, DoubleType())

udf_start = time.time()
featured_df = joined_df.withColumn("avg_speed_mph", speed_udf(
    col("trip_distance"),
    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime")
))
udf_setup_time = time.time() - udf_start

# 6. Export to Parquet
print("[SPARK] Writing transformed data to disk...")
output_path = f"{data_dir}/output_spark"
featured_df.write.mode("overwrite").parquet(output_path)

total_wall_clock = time.time() - start_wall_clock

print("\n" + "="*50)
print("SPARK BENCHMARK RESULTS:")
print(f"Total Wall-Clock Time: {total_wall_clock:.2f} seconds")
print(f"UDF Setup Time: {udf_setup_time:.4f} seconds")
print("="*50 + "\n")

spark.stop()
