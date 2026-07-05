# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: PSX KSE-100 KPI Aggregations
# MAGIC
# MAGIC **Layer**: Gold (Business-Ready Analytics)
# MAGIC **Input**: `de_project_catalog.silver.clean_psx_stocks`
# MAGIC **Output Tables**:
# MAGIC - `de_project_catalog.gold.agg_psx_daily_returns`
# MAGIC - `de_project_catalog.gold.agg_psx_52week_high_low`
# MAGIC - `de_project_catalog.gold.agg_psx_sector_performance`
# MAGIC - `de_project_catalog.gold.agg_psx_volatility_index`
# MAGIC
# MAGIC ## Business Questions This Layer Answers
# MAGIC - Which KSE-100 sectors outperformed in the last 12 months?
# MAGIC - What is the most volatile stock this quarter?
# MAGIC - Which stocks are near their 52-week low (buy signal)?
# MAGIC - How does cement sector performance correlate with construction output?
# MAGIC
# MAGIC > STATUS: Stub — to be built in Session 3 after Silver layer is complete

# COMMAND ----------

# TODO (Session 3):
# Gold Table 1: agg_psx_daily_returns
#   - daily_return = (close - prev_close) / prev_close * 100
#   - Partitioned by sector, date

# Gold Table 2: agg_psx_52week_high_low
#   - Rolling 52-week max(high), min(low) per symbol
#   - Pct from 52wk high/low columns

# Gold Table 3: agg_psx_sector_performance
#   - Average daily return per sector per month
#   - Ranked sector leaderboard

# Gold Table 4: agg_psx_volatility_index
#   - Rolling 30-day std deviation of daily returns per symbol
#   - Beta vs KSE-100 index

pass
