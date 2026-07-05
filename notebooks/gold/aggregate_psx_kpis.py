# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: PSX KSE-100 KPI Aggregations
# MAGIC
# MAGIC **Layer**: Gold (Business-Ready Analytics)
# MAGIC **Input**: `azure_de_project_ws.silver.clean_psx_stocks`
# MAGIC **Output**: Multiple Gold tables
# MAGIC - `azure_de_project_ws.gold.agg_psx_daily_returns`
# MAGIC - `azure_de_project_ws.gold.agg_psx_52week_high_low`
# MAGIC - `azure_de_project_ws.gold.agg_psx_sector_performance`
# MAGIC - `azure_de_project_ws.gold.agg_psx_volatility_index`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold.aggregate_psx_kpis")

spark = SparkSession.builder.getOrCreate()

CATALOG_NAME    = "azure_de_project_ws"
SILVER_SCHEMA   = "silver"
GOLD_SCHEMA     = "gold"
SILVER_TABLE    = "clean_psx_stocks"

SILVER_TABLE_PATH = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{SILVER_TABLE}"

logger.info(f"Reading from Silver Table: {SILVER_TABLE_PATH}")

# Ensure Gold Schema exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Silver Data

# COMMAND ----------

try:
    silver_df = spark.read.table(SILVER_TABLE_PATH)
    # Filter out anomalous data for accurate business aggregations
    clean_df = silver_df.filter(F.col("is_anomalous_return") == False)
    logger.info(f"Loaded {clean_df.count()} clean rows from Silver layer.")
except Exception as e:
    logger.error(f"Failed to load Silver data: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Table 1: Daily Returns Fact Table
# MAGIC Simple projection of the most important daily metrics.

# COMMAND ----------

daily_returns_df = clean_df.select(
    "symbol", "company", "sector", "date", 
    "close", "volume", "daily_return", "price_volatility"
)

# Write to Gold
daily_returns_path = f"{CATALOG_NAME}.{GOLD_SCHEMA}.agg_psx_daily_returns"
daily_returns_df.write.format("delta").mode("overwrite").saveAsTable(daily_returns_path)
logger.info(f"Successfully wrote {daily_returns_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Table 2: 52-Week High & Low
# MAGIC Calculates the absolute highest and lowest price over a rolling 365-day window.

# COMMAND ----------

# Window definition: partition by symbol, order by date, look back 365 days
# 365 days = 31536000 seconds. In PySpark, rangeBetween using dates works with days as integers.
window_52_weeks = Window.partitionBy("symbol").orderBy(F.col("date").cast("timestamp").cast("long")).rangeBetween(-31536000, 0)

high_low_df = (
    clean_df
    .withColumn("52_week_high", F.max("high").over(window_52_weeks))
    .withColumn("52_week_low", F.min("low").over(window_52_weeks))
    # Filter down to just the latest date available per stock
)

# We want the *current* 52-week status, so we only take the most recent record per stock
latest_window = Window.partitionBy("symbol").orderBy(F.col("date").desc())

current_high_low_df = (
    high_low_df
    .withColumn("row_num", F.row_number().over(latest_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .select(
        "symbol", "company", "sector", "date", "close", 
        "52_week_high", "52_week_low"
    )
    .withColumn("pct_from_52wk_high", ((F.col("close") - F.col("52_week_high")) / F.col("52_week_high")) * 100)
    .withColumn("pct_from_52wk_low", ((F.col("close") - F.col("52_week_low")) / F.col("52_week_low")) * 100)
)

# Write to Gold
high_low_path = f"{CATALOG_NAME}.{GOLD_SCHEMA}.agg_psx_52week_high_low"
current_high_low_df.write.format("delta").mode("overwrite").saveAsTable(high_low_path)
logger.info(f"Successfully wrote {high_low_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Table 3: Sector Performance
# MAGIC Monthly aggregation of sector returns.

# COMMAND ----------

sector_perf_df = (
    clean_df
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    .groupBy("sector", "year_month")
    .agg(
        F.avg("daily_return").alias("avg_daily_return_pct"),
        F.sum("volume").alias("total_monthly_volume"),
        F.count("date").alias("trading_days")
    )
    .orderBy("year_month", F.col("avg_daily_return_pct").desc())
)

# Write to Gold
sector_path = f"{CATALOG_NAME}.{GOLD_SCHEMA}.agg_psx_sector_performance"
sector_perf_df.write.format("delta").mode("overwrite").saveAsTable(sector_path)
logger.info(f"Successfully wrote {sector_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Table 4: 30-Day Volatility Index
# MAGIC Calculates the standard deviation of returns to gauge stock risk.

# COMMAND ----------

# 30 days = 2592000 seconds
window_30_days = Window.partitionBy("symbol").orderBy(F.col("date").cast("timestamp").cast("long")).rangeBetween(-2592000, 0)

volatility_df = (
    clean_df
    .withColumn("rolling_30d_stddev", F.stddev_samp("daily_return").over(window_30_days))
    .select("symbol", "company", "sector", "date", "daily_return", "rolling_30d_stddev")
)

# Write to Gold
vol_path = f"{CATALOG_NAME}.{GOLD_SCHEMA}.agg_psx_volatility_index"
volatility_df.write.format("delta").mode("overwrite").saveAsTable(vol_path)
logger.info(f"Successfully wrote {vol_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

logger.info("All Gold tables successfully created and overwritten.")
display(spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{GOLD_SCHEMA}"))
