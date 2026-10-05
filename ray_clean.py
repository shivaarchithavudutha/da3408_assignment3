import time
import os
import glob
import ray
import pandas as pd

start_wall_clock = time.time()

# 1. Connect to local Ray cluster
ray.init(address="auto", ignore_reinit_error=True)

data_dir = os.path.expanduser("~/da3408_a3_scale/data")

# 2. Ingestion
print("[RAY] Ingesting Parquet and CSV files...")
parquet_files = sorted(glob.glob(f"{data_dir}/raw/yellow_tripdata_*.parquet"))

# Select target columns to avoid schema drift
columns_to_read = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "trip_distance"]
trips_ds = ray.data.read_parquet(parquet_files, columns=columns_to_read)

# Read lookup CSV and cast all text fields to string
zones_df = pd.read_csv(f"{data_dir}/raw/taxi_zone_lookup.csv")
zones_df["LocationID"] = zones_df["LocationID"].astype("int64")
for col_name in ["Borough", "Zone", "service_zone"]:
    zones_df[col_name] = zones_df[col_name].fillna("Unknown").astype(str)

# 3. Cleansing
print("[RAY] Cleansing data (null drops & filtering)...")
def clean_batch(batch: pd.DataFrame) -> pd.DataFrame:
    required = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "trip_distance"]
    batch = batch.dropna(subset=required)
    batch = batch[batch["trip_distance"] > 0]
    return batch

clean_ds = trips_ds.map_batches(clean_batch, batch_format="pandas")

# 4. Broadcast / Heavy Join with Taxi Zones
print("[RAY] Performing Join with Taxi Zones...")
zones_ref = ray.put(zones_df)

def join_zones(batch: pd.DataFrame) -> pd.DataFrame:
    zones = ray.get(zones_ref)
    batch["PULocationID"] = batch["PULocationID"].astype("int64")
    merged = pd.merge(batch, zones, left_on="PULocationID", right_on="LocationID", how="inner")
    
    # Force string types to prevent Arrow conversion errors
    for col_name in ["Borough", "Zone", "service_zone"]:
        merged[col_name] = merged[col_name].astype("string")
    return merged

joined_ds = clean_ds.map_batches(join_zones, batch_format="pandas")

# 5. Custom Python UDF (Average Speed per Hour)
udf_start = time.time()

def calculate_speed_batch(batch: pd.DataFrame) -> pd.DataFrame:
    pickup = pd.to_datetime(batch["tpep_pickup_datetime"])
    dropoff = pd.to_datetime(batch["tpep_dropoff_datetime"])
    duration_hours = (dropoff - pickup).dt.total_seconds() / 3600.0
    
    valid_mask = (duration_hours > 0.0) & (duration_hours <= 24.0)
    batch["avg_speed_mph"] = 0.0
    batch.loc[valid_mask, "avg_speed_mph"] = (batch.loc[valid_mask, "trip_distance"] / duration_hours[valid_mask]).astype("float64")
    return batch

featured_ds = joined_ds.map_batches(calculate_speed_batch, batch_format="pandas")
udf_setup_time = time.time() - udf_start

# 6. Export to Parquet
print("[RAY] Writing transformed data to disk...")
output_path = f"{data_dir}/output_ray"
featured_ds.write_parquet(output_path)

total_wall_clock = time.time() - start_wall_clock

print("\n" + "="*50)
print("RAY BENCHMARK RESULTS:")
print(f"Total Wall-Clock Time: {total_wall_clock:.2f} seconds")
print(f"UDF Setup Time: {udf_setup_time:.4f} seconds")
print("="*50 + "\n")

ray.shutdown()
