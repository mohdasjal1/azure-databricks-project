# Azure Data Engineering Pipeline: PSX Stock Analysis

> End-to-end data engineering portfolio project built on Azure + Databricks + Delta Lake.
> Architecture: Medallion (Bronze -> Silver -> Gold) | Governance: Unity Catalog | Orchestration: ADF

---

## Tech Stack

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)

| Component | Technology |
|-----------|-----------|
| Cloud | Azure |
| Processing | Databricks (PySpark) |
| Storage | ADLS Gen2 + Delta Lake |
| Orchestration | Azure Data Factory (ADF) |
| Governance | Unity Catalog / Hive Metastore |

---

## Dataset
**Pakistan Stock Exchange (PSX) KSE-100 Index**
- We ingested 5 years of daily historical stock data (Open, High, Low, Close, Volume) for blue-chip companies in Pakistan.
- Data is dynamically fetched using the `yfinance` Python library API.

---

## Architecture (Medallion)

**1. Bronze Layer (Ingestion)**
- Dynamically fetches raw JSON/CSV data from the API.
- Enforces strict Delta schema constraints (`mergeSchema=false`).
- Appends data with audit columns (`_ingest_timestamp`, `_batch_id`).

**2. Silver Layer (Transformation)**
- Deduplicates raw records using PySpark Window functions to ensure idempotency.
- Calculates daily percentage returns and price volatility.
- Flags anomalies (e.g., trading days with 0 volume or >15% extreme price swings).
- Merges (`UPSERT`) cleaned data into the Silver Delta table.

**3. Gold Layer (Business KPIs)**
- Aggregates the clean Silver data into business-ready tables for Business Intelligence dashboards:
  - `agg_psx_daily_returns`: Fact table for time-series plotting.
  - `agg_psx_52week_high_low`: Calculates rolling 365-day highs and lows.
  - `agg_psx_sector_performance`: Monthly grouped aggregation to rank sector strength.
  - `agg_psx_volatility_index`: Rolling 30-day standard deviation to gauge stock risk.

---

## Project Structure

```
azure-de-project/
+-- notebooks/
|   +-- bronze/      <- Ingests PSX data from yfinance
|   +-- silver/      <- Cleans, deduplicates, and UPSERTS data
|   +-- gold/        <- Creates aggregated KPI tables
+-- README.md
```

---

## How to Run the Pipeline

1. **Setup Databricks**: Ensure you have an Azure Databricks workspace and an interactive cluster running.
2. **Import Notebooks**: Upload the `notebooks/` directory to your Databricks workspace.
3. **Azure Data Factory**: 
   - Create an Azure Data Factory instance.
   - Create a Linked Service to your Databricks cluster using a Personal Access Token.
   - Create a Pipeline with 3 **Databricks Notebook Activities** linked sequentially:
     `Bronze_Ingest` ➔ `Silver_Transform` ➔ `Gold_Aggregate`
4. **Trigger**: Click "Trigger Now" in Azure Data Factory to watch the pipeline execute end-to-end!

---

## Author

**Data Engineering Bootcamp Student** — Karachi, Pakistan
*Building production-grade portfolio projects with Azure + Databricks.*
