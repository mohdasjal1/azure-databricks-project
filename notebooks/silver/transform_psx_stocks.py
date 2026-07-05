# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: PSX KSE-100 Stock Data Transformation
# MAGIC
# MAGIC **Layer**: Silver (Clean & Validated)
# MAGIC **Input**: `azure_de_project_ws.bronze.raw_psx_stocks`
# MAGIC **Output**: `azure_de_project_ws.silver.clean_psx_stocks`
# MAGIC **Write Mode**: MERGE (UPSERT on symbol + date — Silver deduplication rule)
# MAGIC
# MAGIC ## What This Notebook Will Do
# MAGIC 1. Read from Bronze Delta table
# MAGIC 2. Deduplicate by `(symbol, date)` — latest record wins (based on _ingest_timestamp)
# MAGIC 3. Add derived metrics: `price_volatility` and `daily_return`
# MAGIC 4. Flag anomalous price movements and zero volumes
# MAGIC 5. UPSERT into Silver Delta table
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver.transform_psx_stocks")

spark = SparkSession.builder.getOrCreate()

CATALOG_NAME    = "azure_de_project_ws"
BRONZE_SCHEMA   = "bronze"
SILVER_SCHEMA   = "silver"
TABLE_NAME      = "raw_psx_stocks"       # Input table
SILVER_TABLE    = "clean_psx_stocks"     # Output table

BRONZE_TABLE_PATH = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{TABLE_NAME}"
SILVER_TABLE_PATH = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{SILVER_TABLE}"

logger.info(f"Reading from: {BRONZE_TABLE_PATH}")
logger.info(f"Writing to:   {SILVER_TABLE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze Data

# COMMAND ----------

try:
    bronze_df = spark.read.table(BRONZE_TABLE_PATH)
    logger.info(f"Read {bronze_df.count()} rows from Bronze.")
except Exception as e:
    logger.error(f"Failed to read from Bronze table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deduplicate (symbol, date)

# COMMAND ----------

# Because the Bronze layer is append-only, if we run the ingest script multiple times for the same dates,
# we will get duplicate rows.
# We solve this by Windowing over (symbol, date) and keeping the row with the latest _ingest_timestamp.

dedup_window = Window.partitionBy("symbol", "date").orderBy(F.col("_ingest_timestamp").desc())

dedup_df = (
    bronze_df
    .withColumn("row_num", F.row_number().over(dedup_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

logger.info(f"Row count after deduplication: {dedup_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Derived Metrics & Anomaly Detection

# COMMAND ----------

# To calculate daily returns, we need to look at the previous day's close for the same stock.
# We use another Window function partitioned by symbol and ordered by date.
lag_window = Window.partitionBy("symbol").orderBy("date")

transformed_df = (
    dedup_df
    # 1. Price Volatility: (High - Low) / Open
    .withColumn("price_volatility", 
        F.when(F.col("open") > 0, (F.col("high") - F.col("low")) / F.col("open"))
         .otherwise(0.0)
    )
    
    # 2. Daily Return: (Close - Prev_Close) / Prev_Close * 100
    .withColumn("prev_close", F.lag("close", 1).over(lag_window))
    .withColumn("daily_return", 
        F.when(F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
               ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100)
         .otherwise(0.0)
    )
    .drop("prev_close")
    
    # 3. Anomaly Flags
    .withColumn("is_zero_volume", F.col("volume") <= 0)
    .withColumn("is_anomalous_return", F.abs(F.col("daily_return")) > 15.0) # > 15% jump/drop
    
    # 4. Silver Audit Column
    .withColumn("_silver_update_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Silver Table (If Not Exists)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{SILVER_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE_PATH} (
        symbol                  STRING,
        company                 STRING,
        sector                  STRING,
        date                    DATE,
        open                    DOUBLE,
        high                    DOUBLE,
        low                     DOUBLE,
        close                   DOUBLE,
        volume                  LONG,
        price_volatility        DOUBLE,
        daily_return            DOUBLE,
        is_zero_volume          BOOLEAN,
        is_anomalous_return     BOOLEAN,
        _ingest_timestamp       TIMESTAMP,
        _source                 STRING,
        _batch_id               STRING,
        _silver_update_timestamp TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (sector, date)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Upsert (MERGE) into Silver

# COMMAND ----------

# We use DeltaTable to perform a MERGE INTO operation
silver_table = DeltaTable.forName(spark, SILVER_TABLE_PATH)

(
    silver_table.alias("target")
    .merge(
        transformed_df.alias("source"),
        "target.symbol = source.symbol AND target.date = source.date"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

logger.info("Upsert to Silver complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation

# COMMAND ----------

# Let's count how many anomalies we found
spark.sql(f"""
    SELECT sector, COUNT(*) as anomaly_count
    FROM {SILVER_TABLE_PATH}
    WHERE is_anomalous_return = true OR is_zero_volume = true
    GROUP BY sector
""").show()
