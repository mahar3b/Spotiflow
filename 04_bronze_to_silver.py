# Databricks notebook source
# Databricks Notebook: 04_bronze_to_silver
spark.conf.set("spark.sql.ansi.enabled", "false")

from pyspark.sql.functions import col, when, split, trim, to_date, lit, current_timestamp, md5, concat_ws, coalesce, expr
from datetime import datetime

spark.sql("USE SCHEMA spotipulse_db")

dbutils.widgets.text("process_date", "", "Process Date (YYYY-MM-DD)")
param_date = dbutils.widgets.get("process_date").strip()

def log_execution(layer, param, start_time, end_time, status, rows, error=""):
    log_data = [(layer, param, start_time, end_time, status, int(rows), error)]
    schema = "layer STRING, parameter_processed STRING, start_time TIMESTAMP, end_time TIMESTAMP, status STRING, rows_affected LONG, error_message STRING"
    df_log = spark.createDataFrame(log_data, schema)
    df_log.write.format("delta").mode("append").saveAsTable("pipeline_execution_logs")

# 1. POPULATE DIM_REGION
spark.sql("""
CREATE TABLE IF NOT EXISTS dim_region (
    region_code STRING,
    region_name STRING,
    load_timestamp TIMESTAMP
) USING DELTA;
""")

regions_data = [
    ("global", "Global"), ("us", "United States"), ("gb", "United Kingdom"),
    ("de", "Germany"), ("jp", "Japan"), ("br", "Brazil"),
    ("ca", "Canada"), ("au", "Australia"), ("fr", "France"), ("mx", "Mexico")
]
df_regions = spark.createDataFrame(regions_data, ["region_code", "region_name"]).withColumn("load_timestamp", current_timestamp())
df_regions.createOrReplaceTempView("temp_regions")

spark.sql("""
MERGE INTO dim_region target
USING temp_regions source
ON target.region_code = source.region_code
WHEN NOT MATCHED THEN INSERT *
""")

# 2. RE-CREATE FACT_CHART_ENTRY WITH SURROGATE TRACK_ID HANDLING
spark.sql("""
CREATE TABLE IF NOT EXISTS fact_chart_entry (
    track_id STRING,
    artist_name STRING,
    track_title STRING,
    chart_rank INT,
    trend_type STRING,
    stream_count LONG,
    entry_date DATE,
    region_code STRING,
    data_source STRING,
    load_timestamp TIMESTAMP
) USING DELTA;
""")

start = datetime.now()
filter_clause = f"WHERE chart_date = '{param_date}'" if param_date else ""

try:
    # Filter out any corrupt/empty date strings from bronze
    df_kworb = spark.sql(f"SELECT * FROM bronze_kworb_charts {filter_clause}") \
                    .filter("chart_date IS NOT NULL AND trim(chart_date) != ''")

    # Extract clean fields & derive track_id if null
    df_cleaned = df_kworb.withColumn("artist_name", trim(split(col("artist_title"), "-").getItem(0))) \
                         .withColumn("track_title", trim(split(col("artist_title"), "-").getItem(1))) \
                         .withColumn("clean_track_id", 
                             coalesce(col("track_id"), md5(concat_ws("||", col("artist_name"), col("track_title"))))
                         )

    # Clean & Deduplicate Records using safe try_cast for Photon/ANSI compatibility
    df_silver_kworb = df_cleaned.dropDuplicates(["clean_track_id", "chart_date", "region"]).select(
        col("clean_track_id").alias("track_id"),
        col("artist_name"),
        col("track_title"),
        expr("try_cast(pos AS INT)").alias("chart_rank"),
        when(col("trend") == "=", "SAME_POSITION")
        .when(col("trend").startswith("+"), "MOVE_UP")
        .when(col("trend").startswith("-"), "MOVE_DOWN")
        .when(col("trend") == "RE", "RE_ENTRY")
        .otherwise("NEW_ENTRY").alias("trend_type"),
        expr("try_cast(streams AS BIGINT)").alias("stream_count"),
        expr("try_cast(chart_date AS DATE)").alias("entry_date"),
        col("region").alias("region_code"),
        lit("kworb_incremental").alias("data_source"),
        current_timestamp().alias("load_timestamp")
    ).filter("entry_date IS NOT NULL")

    df_silver_kworb.createOrReplaceTempView("temp_silver_kworb")

    # Idempotent MERGE Statement
    merge_result = spark.sql("""
    MERGE INTO fact_chart_entry target
    USING temp_silver_kworb source
    ON target.track_id = source.track_id 
       AND target.entry_date = source.entry_date 
       AND target.region_code = source.region_code
    WHEN MATCHED THEN UPDATE SET 
        target.artist_name = source.artist_name,
        target.track_title = source.track_title,
        target.chart_rank = source.chart_rank,
        target.trend_type = source.trend_type,
        target.stream_count = source.stream_count,
        target.load_timestamp = source.load_timestamp
    WHEN NOT MATCHED THEN INSERT *
    """)

    affected_rows = merge_result.collect()[0]["num_affected_rows"]
    log_execution("SILVER_FACT", param_date if param_date else "LATEST", start, datetime.now(), "SUCCESS", affected_rows)
    print(f"✔ Silver layer 'fact_chart_entry' updated successfully ({affected_rows} rows affected).")

except Exception as e:
    log_execution("SILVER_FACT", param_date if param_date else "LATEST", start, datetime.now(), "FAILED", 0, str(e))
    print(f"Silver merge issue: {e}")
