# Databricks notebook source
# Databricks Notebook: 05_silver_to_gold
from pyspark.sql.functions import col, sum, count, min, max, first, expr, current_timestamp, date_format, dayofweek, year, month, dayofmonth
from datetime import datetime

spark.sql("USE SCHEMA spotipulse_db")

# Parameter Widget for Backfilling
dbutils.widgets.text("process_date", "", "Process Date (YYYY-MM-DD)")
param_date = dbutils.widgets.get("process_date").strip()

def log_execution(layer, param, start_time, end_time, status, rows, error=""):
    log_data = [(layer, param, start_time, end_time, status, int(rows), error)]
    schema = "layer STRING, parameter_processed STRING, start_time TIMESTAMP, end_time TIMESTAMP, status STRING, rows_affected LONG, error_message STRING"
    df_log = spark.createDataFrame(log_data, schema)
    df_log.write.format("delta").mode("append").saveAsTable("pipeline_execution_logs")

start = datetime.now()

try:
    # 1. POPULATE DIM_DATE
    spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key DATE,
        year INT,
        month INT,
        month_name STRING,
        day INT,
        day_of_week STRING,
        is_weekend BOOLEAN
    ) USING DELTA;
    """)

    df_dates = spark.sql("SELECT DISTINCT entry_date FROM fact_chart_entry WHERE entry_date IS NOT NULL")
    df_dim_date = df_dates.select(
        col("entry_date").alias("date_key"),
        year("entry_date").alias("year"),
        month("entry_date").alias("month"),
        date_format("entry_date", "MMMM").alias("month_name"),
        dayofmonth("entry_date").alias("day"),
        date_format("entry_date", "EEEE").alias("day_of_week"),
        expr("CASE WHEN dayofweek(entry_date) IN (1, 7) THEN true ELSE false END").alias("is_weekend")
    )
    
    df_dim_date.createOrReplaceTempView("temp_dim_date")
    spark.sql("""
    MERGE INTO dim_date target
    USING temp_dim_date source
    ON target.date_key = source.date_key
    WHEN NOT MATCHED THEN INSERT *
    """)

    # 2. POPULATE GOLD DAILY CHART KPIS
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gold_daily_chart_kpis (
        entry_date DATE,
        region_code STRING,
        total_streams LONG,
        total_active_tracks INT,
        load_timestamp TIMESTAMP
    ) USING DELTA
    CLUSTER BY (entry_date, region_code);
    """)

    df_facts = spark.sql(f"SELECT * FROM fact_chart_entry {f'WHERE entry_date = \"{param_date}\"' if param_date else ''}")

    df_gold_kpis = df_facts.groupBy("entry_date", "region_code").agg(
        sum("stream_count").alias("total_streams"),
        count("track_id").alias("total_active_tracks")
    ).withColumn("load_timestamp", current_timestamp())

    df_gold_kpis.createOrReplaceTempView("temp_gold_kpis")

    merge_kpi_res = spark.sql("""
    MERGE INTO gold_daily_chart_kpis target
    USING temp_gold_kpis source
    ON target.entry_date = source.entry_date AND target.region_code = source.region_code
    WHEN MATCHED THEN UPDATE SET
        target.total_streams = source.total_streams,
        target.total_active_tracks = source.total_active_tracks,
        target.load_timestamp = source.load_timestamp
    WHEN NOT MATCHED THEN INSERT *
    """)

    affected_kpi_rows = merge_kpi_res.collect()[0]["num_affected_rows"]

    # 3. POPULATE GOLD TRACK PERFORMANCE AGGREGATION
    spark.sql("""
    CREATE TABLE IF NOT EXISTS gold_track_performance (
        track_id STRING,
        artist_name STRING,
        track_title STRING,
        first_entry_date DATE,
        latest_entry_date DATE,
        days_on_chart INT,
        peak_rank INT,
        cumulative_streams LONG,
        load_timestamp TIMESTAMP
    ) USING DELTA
    CLUSTER BY (track_id);
    """)

    df_track_perf = spark.sql("""
    SELECT 
        track_id,
        FIRST(artist_name) AS artist_name,
        FIRST(track_title) AS track_title,
        MIN(entry_date) AS first_entry_date,
        MAX(entry_date) AS latest_entry_date,
        COUNT(DISTINCT entry_date) AS days_on_chart,
        MIN(chart_rank) AS peak_rank,
        SUM(stream_count) AS cumulative_streams,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM fact_chart_entry
    GROUP BY track_id
    """)

    df_track_perf.createOrReplaceTempView("temp_track_perf")

    merge_track_res = spark.sql("""
    MERGE INTO gold_track_performance target
    USING temp_track_perf source
    ON target.track_id = source.track_id
    WHEN MATCHED THEN UPDATE SET
        target.artist_name = source.artist_name,
        target.track_title = source.track_title,
        target.first_entry_date = source.first_entry_date,
        target.latest_entry_date = source.latest_entry_date,
        target.days_on_chart = source.days_on_chart,
        target.peak_rank = source.peak_rank,
        target.cumulative_streams = source.cumulative_streams,
        target.load_timestamp = source.load_timestamp
    WHEN NOT MATCHED THEN INSERT *
    """)

    affected_track_rows = merge_track_res.collect()[0]["num_affected_rows"]

    # Compact tables
    spark.sql("OPTIMIZE gold_daily_chart_kpis")
    spark.sql("OPTIMIZE gold_track_performance")

    total_affected = affected_kpi_rows + affected_track_rows
    log_execution("GOLD_LAYER", param_date if param_date else "FULL_REFRESH", start, datetime.now(), "SUCCESS", total_affected)
    print(f"✔ Gold layer successfully populated ({total_affected} total rows affected across Gold tables).")

except Exception as e:
    log_execution("GOLD_LAYER", param_date if param_date else "FULL_REFRESH", start, datetime.now(), "FAILED", 0, str(e))
    print(f"✖ Gold layer error: {e}")
