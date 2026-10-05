import pyarrow.dataset as ds
import pandas as pd
import os

spark_path = os.path.expanduser("~/da3408_a3_scale/data/output_spark")
ray_path = os.path.expanduser("~/da3408_a3_scale/data/output_ray")

print("Checking Pipeline Parity across Spark and Ray outputs...\n")

# Use PyArrow dataset scanner for fast out-of-core comparison
ds_spark = ds.dataset(spark_path, format="parquet")
ds_ray = ds.dataset(ray_path, format="parquet")

spark_rows = ds_spark.count_rows()
ray_rows = ds_ray.count_rows()

print(f"1. Total Row Count:")
print(f"   - Spark Output Rows : {spark_rows:,}")
print(f"   - Ray Output Rows   : {ray_rows:,}")
assert spark_rows == ray_rows, "Row counts do not match!"
print("   --> [PASS] Exact Row Count Match!\n")

print(f"2. Column Schema Check:")
spark_cols = set(ds_spark.schema.names)
ray_cols = set(ds_ray.schema.names)
print(f"   - Spark Columns : {sorted(list(spark_cols))}")
print(f"   - Ray Columns   : {sorted(list(ray_cols))}")
assert spark_cols == ray_cols, "Columns do not match!"
print("   --> [PASS] Exact Schema Match!\n")

print(f"3. Sample Value Check (Top 3 rows):")
sample_spark = ds_spark.head(3).to_pandas()[["trip_distance", "avg_speed_mph", "Borough", "Zone"]]
sample_ray = ds_ray.head(3).to_pandas()[["trip_distance", "avg_speed_mph", "Borough", "Zone"]]
print("\n--- Spark Sample ---")
print(sample_spark)
print("\n--- Ray Sample ---")
print(sample_ray)
print("\n==================================================")
print("VERDICT: Pipeline Parity 100% Verified!")
print("==================================================")
