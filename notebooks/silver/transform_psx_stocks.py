# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: PSX KSE-100 Stock Data Transformation
# MAGIC
# MAGIC **Layer**: Silver (Clean & Validated)
# MAGIC **Input**: `de_project_catalog.bronze.raw_psx_stocks`
# MAGIC **Output**: `de_project_catalog.silver.clean_psx_stocks`
# MAGIC **Write Mode**: MERGE (UPSERT on symbol + date — Silver deduplication rule)
# MAGIC
# MAGIC ## What This Notebook Will Do
# MAGIC 1. Read from Bronze Delta table (incremental — only new batches)
# MAGIC 2. Validate and cast column types
# MAGIC 3. Deduplicate by `(symbol, date)` — latest record wins
# MAGIC 4. Flag anomalous price movements (circuit breakers, data errors)
# MAGIC 5. Standardize sector and company name formats
# MAGIC 6. UPSERT into Silver Delta table
# MAGIC
# MAGIC > STATUS: Stub — to be built in Session 2 after Bronze is validated in Databricks

# COMMAND ----------

# TODO (Session 2):
# - Read Bronze table with incremental watermark on _ingest_timestamp
# - Apply deduplication logic: window function over (symbol, date), keep latest close
# - Cast price columns to DoubleType, volume to LongType, date to DateType
# - Add derived column: daily_return = (close - lag(close)) / lag(close) * 100
# - Flag rows where abs(daily_return) > 15% as anomalous (circuit breaker threshold)
# - MERGE into silver table on (symbol, date) primary key
# - Add Unity Catalog column-level tags: mark 'close' as financial_data

pass
