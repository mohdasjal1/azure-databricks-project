# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: PSX KSE-100 Stock Data Ingestion
# MAGIC
# MAGIC **Layer**: Bronze (Raw Ingestion)
# MAGIC **Domain**: Pakistan Stock Exchange (PSX) — KSE-100 Financial Data
# MAGIC **Source**: Yahoo Finance API via `yfinance` Python library
# MAGIC **Output**: Delta Lake table — `de_project_catalog.bronze.raw_psx_stocks`
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. Defines the list of KSE-100 blue-chip tickers by sector
# MAGIC 2. Fetches historical OHLCV data from Yahoo Finance (configurable date range)
# MAGIC 3. Adds mandatory metadata columns: `_ingest_timestamp`, `_source`, `_batch_id`
# MAGIC 4. Writes data to Bronze Delta table (append-only)
# MAGIC 5. Logs row counts and validates output
# MAGIC
# MAGIC ## Why Bronze Is Append-Only
# MAGIC The Bronze layer stores raw data exactly as received. We never modify or delete Bronze records.
# MAGIC If source data changes, we ingest again and Silver handles deduplication via UPSERT.
# MAGIC This gives us full audit history and the ability to reprocess from scratch at any time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# Install yfinance — Yahoo Finance API wrapper (free, no API key required)
# yfinance is not pre-installed on Databricks, so we install it at notebook startup
%pip install yfinance==0.2.36 --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports and Configuration

# COMMAND ----------

import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, DateType, TimestampType
)
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import uuid
import logging

# Configure logging for this notebook
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze.ingest_psx_stocks")

# Spark session — already available in Databricks as `spark`
spark = SparkSession.builder.getOrCreate()

# Notebook-level configuration
# WHY: Centralizing config here makes it easy to parameterize via ADF pipeline later
# ADF will pass these as notebook parameters when orchestrating
CATALOG_NAME    = "de_project_catalog"   # Unity Catalog catalog name
SCHEMA_NAME     = "bronze"               # Medallion layer schema
TABLE_NAME      = "raw_psx_stocks"       # Full table: de_project_catalog.bronze.raw_psx_stocks
FULL_TABLE_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

# Date range — fetch last 5 years of history on first run
# ADF can override INGEST_START_DATE for incremental daily runs
INGEST_END_DATE   = date.today().strftime("%Y-%m-%d")
INGEST_START_DATE = (date.today() - relativedelta(years=5)).strftime("%Y-%m-%d")

# Unique batch ID for this ingestion run — used for lineage tracking and debugging
BATCH_ID = str(uuid.uuid4())

logger.info(f"Batch ID: {BATCH_ID}")
logger.info(f"Ingestion window: {INGEST_START_DATE} to {INGEST_END_DATE}")
logger.info(f"Target table: {FULL_TABLE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. KSE-100 Ticker Registry
# MAGIC
# MAGIC We use a curated list of blue-chip KSE-100 companies covering 5 major sectors.
# MAGIC Yahoo Finance appends `.KA` to all PSX tickers (KA = Karachi, Pakistan).
# MAGIC
# MAGIC **Why these tickers?**
# MAGIC These 15 companies represent ~60% of KSE-100 market capitalization.
# MAGIC They are the most liquid, have the longest price history, and are least likely to be
# MAGIC delisted or have data gaps — making them ideal for a reliable data pipeline.

# COMMAND ----------

# KSE-100 blue-chip tickers by sector — each with Yahoo Finance symbol and company metadata
KSE100_TICKERS = [
    # BANKS — Pakistan's largest sector by market cap
    {"symbol": "HBL.KA",   "company": "Habib Bank Limited",          "sector": "Banks"},
    {"symbol": "UBL.KA",   "company": "United Bank Limited",          "sector": "Banks"},
    {"symbol": "MCB.KA",   "company": "MCB Bank Limited",             "sector": "Banks"},
    {"symbol": "BAFL.KA",  "company": "Bank Alfalah Limited",         "sector": "Banks"},

    # ENERGY — Oil & Gas exploration, critical to Pakistan's economy
    {"symbol": "PSO.KA",   "company": "Pakistan State Oil Company",   "sector": "Energy"},
    {"symbol": "OGDC.KA",  "company": "Oil & Gas Dev. Company",       "sector": "Energy"},
    {"symbol": "PPL.KA",   "company": "Pakistan Petroleum Limited",   "sector": "Energy"},

    # FERTILIZER — Agriculture-linked, cyclical sector
    {"symbol": "ENGRO.KA", "company": "Engro Corporation Limited",    "sector": "Fertilizer"},
    {"symbol": "FFC.KA",   "company": "Fauji Fertilizer Company",     "sector": "Fertilizer"},
    {"symbol": "EFERT.KA", "company": "Engro Fertilizers Limited",    "sector": "Fertilizer"},

    # CEMENT — Infrastructure and construction bellwether
    {"symbol": "LUCK.KA",  "company": "Lucky Cement Limited",         "sector": "Cement"},
    {"symbol": "DGKC.KA",  "company": "D.G. Khan Cement Company",     "sector": "Cement"},
    {"symbol": "MLCF.KA",  "company": "Maple Leaf Cement Factory",    "sector": "Cement"},

    # TECHNOLOGY / TELECOM — Emerging growth sector
    {"symbol": "TRG.KA",   "company": "TRG Pakistan Limited",         "sector": "Technology"},
    {"symbol": "SYS.KA",   "company": "Systems Limited",              "sector": "Technology"},
]

ticker_symbols = [t["symbol"] for t in KSE100_TICKERS]
ticker_metadata = {t["symbol"]: {"company": t["company"], "sector": t["sector"]} for t in KSE100_TICKERS}

logger.info(f"Tickers to ingest: {len(ticker_symbols)}")
print(f"Sectors: {set(t['sector'] for t in KSE100_TICKERS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fetch Data from Yahoo Finance
# MAGIC
# MAGIC We use `yfinance.download()` with `group_by="ticker"` to fetch all tickers in a single API call.
# MAGIC This is more efficient than looping and calling the API per ticker.
# MAGIC
# MAGIC **Why not use PSX's own API?**
# MAGIC PSX's official data portal requires registration and paid subscription for bulk historical data.
# MAGIC Yahoo Finance provides the same OHLCV data for free via yfinance, which is acceptable for a
# MAGIC portfolio/educational project. In production, you would integrate with Bloomberg or Refinitiv.

# COMMAND ----------

def fetch_psx_ohlcv(
    symbols: list,
    start_date: str,
    end_date: str,
    metadata_map: dict
) -> pd.DataFrame:
    """
    Fetch OHLCV data for a list of PSX tickers from Yahoo Finance.

    Args:
        symbols: List of Yahoo Finance ticker symbols (e.g., ["HBL.KA", "ENGRO.KA"])
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        metadata_map: Dict mapping symbol -> {company, sector}

    Returns:
        Pandas DataFrame with columns:
        [symbol, company, sector, date, open, high, low, close, volume]
    """
    logger.info(f"Fetching Yahoo Finance data: {len(symbols)} tickers, {start_date} to {end_date}")

    try:
        # Download all tickers in one API call for efficiency
        raw = yf.download(
            tickers=symbols,
            start=start_date,
            end=end_date,
            group_by="ticker",
            auto_adjust=True,    # Adjust for splits and dividends automatically
            threads=True,         # Parallel download
            progress=False        # Suppress progress bar (not needed in pipeline)
        )
    except Exception as e:
        logger.error(f"Yahoo Finance API call failed: {e}")
        raise

    all_records = []

    for symbol in symbols:
        try:
            # Extract per-ticker DataFrame from the multi-level column structure
            if len(symbols) > 1:
                ticker_df = raw[symbol].copy()
            else:
                ticker_df = raw.copy()

            # Skip if no data returned (delisted or unavailable ticker)
            if ticker_df.empty:
                logger.warning(f"No data returned for {symbol} — skipping")
                continue

            # Reset index so Date becomes a column (not index)
            ticker_df = ticker_df.reset_index()

            # Rename columns to lowercase snake_case — convention for all our Delta tables
            ticker_df.columns = [c.lower().replace(" ", "_") for c in ticker_df.columns]

            # Add ticker identifier and metadata
            ticker_df["symbol"]  = symbol
            ticker_df["company"] = metadata_map[symbol]["company"]
            ticker_df["sector"]  = metadata_map[symbol]["sector"]

            # Select and order final columns
            # WHY: Explicit column selection prevents schema drift if yfinance adds new fields
            ticker_df = ticker_df[[
                "symbol", "company", "sector", "date",
                "open", "high", "low", "close", "volume"
            ]]

            all_records.append(ticker_df)
            logger.info(f"  {symbol}: {len(ticker_df)} rows fetched")

        except KeyError as e:
            logger.warning(f"Column error for {symbol}: {e} — skipping")
            continue
        except Exception as e:
            logger.error(f"Unexpected error processing {symbol}: {e}")
            continue

    if not all_records:
        raise ValueError("No data fetched for any ticker. Check ticker symbols and date range.")

    combined = pd.concat(all_records, ignore_index=True)
    logger.info(f"Total rows fetched: {len(combined)}")
    return combined


# Execute the fetch
raw_pdf = fetch_psx_ohlcv(
    symbols=ticker_symbols,
    start_date=INGEST_START_DATE,
    end_date=INGEST_END_DATE,
    metadata_map=ticker_metadata
)

# Preview the raw data
print(f"\nRaw data shape: {raw_pdf.shape}")
display(raw_pdf.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add Bronze Metadata Columns
# MAGIC
# MAGIC Every Bronze table MUST have these three metadata columns. They are the audit trail.
# MAGIC
# MAGIC | Column | Purpose |
# MAGIC |--------|---------|
# MAGIC | `_ingest_timestamp` | Exact UTC timestamp when this row was written — for debugging and incremental loads |
# MAGIC | `_source` | Where the data came from — critical for data lineage in Unity Catalog |
# MAGIC | `_batch_id` | UUID for this specific ingestion run — lets you trace any row back to its run |

# COMMAND ----------

def add_bronze_metadata(sdf, source: str, batch_id: str):
    """
    Add mandatory Bronze layer metadata columns to the Spark DataFrame.

    Args:
        sdf: Input Spark DataFrame
        source: String identifying data source (for lineage)
        batch_id: UUID string for this ingestion batch

    Returns:
        Spark DataFrame with metadata columns appended
    """
    return (
        sdf
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source",           F.lit(source))
        .withColumn("_batch_id",         F.lit(batch_id))
    )


# Convert Pandas DataFrame to Spark DataFrame
# WHY: All Delta Lake writes in Databricks use Spark DataFrames — not Pandas
raw_sdf = spark.createDataFrame(raw_pdf)

# Add metadata columns
bronze_sdf = add_bronze_metadata(
    sdf=raw_sdf,
    source="yahoo_finance_api/yfinance_v0.2.36",
    batch_id=BATCH_ID
)

# Print schema so we can verify column types before writing
print("Bronze DataFrame Schema:")
bronze_sdf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Define Bronze Delta Table Schema
# MAGIC
# MAGIC We explicitly define the schema via Unity Catalog DDL rather than relying on schema inference.
# MAGIC
# MAGIC **Why explicit schema?**
# MAGIC - Prevents silent schema drift — if yfinance changes column types, the write fails loudly
# MAGIC - Unity Catalog enforces this schema as the contract for the Bronze layer
# MAGIC - Makes the Silver layer's type expectations clear and testable

# COMMAND ----------

# WHY: We use SQL DDL to create the table through Unity Catalog so that:
# 1. The table is registered in the metastore with proper governance
# 2. Schema is enforced from day one
# 3. Delta Lake features (time travel, ACID) are enabled automatically

# First, create the catalog and schema if they don't exist
# On first run this is needed; subsequent runs will skip these DDL statements
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

# Create the Bronze Delta table with explicit schema
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE_PATH} (
        symbol              STRING          COMMENT 'Yahoo Finance ticker symbol (e.g. HBL.KA)',
        company             STRING          COMMENT 'Full company name',
        sector              STRING          COMMENT 'PSX sector classification',
        date                DATE            COMMENT 'Trading date (YYYY-MM-DD)',
        open                DOUBLE          COMMENT 'Opening price in PKR',
        high                DOUBLE          COMMENT 'Intraday high price in PKR',
        low                 DOUBLE          COMMENT 'Intraday low price in PKR',
        close               DOUBLE          COMMENT 'Closing/adjusted price in PKR',
        volume              LONG            COMMENT 'Number of shares traded',
        _ingest_timestamp   TIMESTAMP       COMMENT 'UTC timestamp when row was ingested',
        _source             STRING          COMMENT 'Data source identifier for lineage',
        _batch_id           STRING          COMMENT 'UUID for this ingestion batch run'
    )
    USING DELTA
    PARTITIONED BY (sector, date)
    COMMENT 'Bronze: Raw PSX KSE-100 OHLCV data ingested from Yahoo Finance API. Append-only.'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'team'                             = 'data-engineering',
        'layer'                            = 'bronze',
        'domain'                           = 'financial-markets',
        'source'                           = 'yahoo-finance-api',
        'pii'                              = 'false'
    )
""")

logger.info(f"Table {FULL_TABLE_PATH} created or already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Bronze Delta Table (Append-Only)
# MAGIC
# MAGIC **Write mode: APPEND**
# MAGIC The Bronze layer is always append-only. We never update or delete raw records.
# MAGIC If you need to re-ingest a date range, the Silver layer handles deduplication via UPSERT.
# MAGIC
# MAGIC **Why append?**
# MAGIC - Preserves full audit history of every ingestion run
# MAGIC - Simplifies Bronze logic — no merge logic needed at this layer
# MAGIC - Silver is the single source of deduplication truth

# COMMAND ----------

def write_bronze_delta(sdf, table_path: str, batch_id: str) -> int:
    """
    Write Spark DataFrame to Bronze Delta table in append mode.

    Args:
        sdf: Bronze Spark DataFrame (with metadata columns)
        table_path: Full table path (catalog.schema.table)
        batch_id: Batch ID for logging

    Returns:
        Number of rows written
    """
    row_count = sdf.count()
    logger.info(f"Writing {row_count} rows to {table_path} (batch: {batch_id})")

    try:
        (
            sdf.write
            .format("delta")
            .mode("append")                # Append-only — Bronze layer rule
            .option("mergeSchema", "false") # Reject schema changes — fail loudly
            .saveAsTable(table_path)
        )
        logger.info(f"Write successful: {row_count} rows appended to {table_path}")
        return row_count

    except Exception as e:
        logger.error(f"Delta write failed for batch {batch_id}: {e}")
        raise


rows_written = write_bronze_delta(
    sdf=bronze_sdf,
    table_path=FULL_TABLE_PATH,
    batch_id=BATCH_ID
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Output and Log Run Summary
# MAGIC
# MAGIC Post-write validation is mandatory in production pipelines.
# MAGIC We verify the data landed correctly before marking the pipeline run as successful.

# COMMAND ----------

# Read back a sample from the written table to confirm data integrity
print("=" * 60)
print("BRONZE INGESTION RUN SUMMARY")
print("=" * 60)
print(f"Batch ID         : {BATCH_ID}")
print(f"Tickers ingested : {len(ticker_symbols)}")
print(f"Date range       : {INGEST_START_DATE} to {INGEST_END_DATE}")
print(f"Rows written     : {rows_written}")
print(f"Target table     : {FULL_TABLE_PATH}")
print("=" * 60)

# Row count by sector — quick sanity check
print("\nRows by sector (this batch):")
(
    bronze_sdf
    .groupBy("sector")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("symbol").alias("ticker_count"),
        F.min("date").alias("earliest_date"),
        F.max("date").alias("latest_date")
    )
    .orderBy("sector")
    .show()
)

# Total table row count (all batches, not just this one)
total_rows = spark.table(FULL_TABLE_PATH).count()
print(f"\nTotal rows in {FULL_TABLE_PATH} (all batches): {total_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Delta Lake Time Travel Verification
# MAGIC
# MAGIC Delta Lake automatically versions every write. We can query the table history to confirm
# MAGIC our write was recorded. This is a key feature we will use in Silver for incremental loads.

# COMMAND ----------

# Show Delta table history — confirms write was recorded with metadata
print("Delta Table History (last 5 operations):")
spark.sql(f"DESCRIBE HISTORY {FULL_TABLE_PATH} LIMIT 5").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Complete
# MAGIC
# MAGIC **Output**: `de_project_catalog.bronze.raw_psx_stocks` — Delta Lake table
# MAGIC
# MAGIC **Next Step**: Run `silver/transform_psx_stocks.py` to:
# MAGIC - Deduplicate by `(symbol, date)` using MERGE/UPSERT
# MAGIC - Cast and validate column types
# MAGIC - Flag any anomalous price movements
# MAGIC - Write to `de_project_catalog.silver.clean_psx_stocks`
