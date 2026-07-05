# Azure Data Engineering Project — Architecture

## Overview

End-to-end data engineering pipeline built on Azure + Databricks using the Medallion Architecture
(Bronze -> Silver -> Gold). Demonstrates production-grade data engineering skills including
ingestion, transformation, orchestration, governance, and CI/CD.

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Cloud Platform | Azure | Hosting all infrastructure |
| Processing Engine | Databricks (Free Edition) | PySpark-based transformations |
| Orchestration | Azure Data Factory | Pipeline scheduling and triggering |
| Storage | ADLS Gen2 + Delta Lake | Medallion architecture storage |
| Governance | Unity Catalog | Schema enforcement, lineage, RBAC |
| CI/CD | GitHub Actions | Notebook + ADF deployment automation |
| Secret Management | Azure Key Vault | No secrets in code, ever |

---

## Medallion Layer Definitions

### Bronze - Raw Ingestion
- Raw data as-is from source + metadata columns (_ingest_timestamp, _source_file, _batch_id)
- Write mode: Append-only | Format: Delta Lake

### Silver - Clean and Validated
- Type casting, null handling, deduplication, string normalization
- Write mode: UPSERT on primary key | Format: Delta Lake

### Gold - Business Ready
- Aggregated KPIs, star schema, business metrics
- Write mode: Overwrite or incremental | Format: Delta Lake

---

## Unity Catalog Structure

```
Catalog: de_project_catalog
+-- Schema: bronze  ->  raw_<domain>_<entity>
+-- Schema: silver  ->  clean_<domain>_<entity>
+-- Schema: gold    ->  agg_<domain>_<kpi>
```

---

## Dataset
> To be filled after dataset selection in Session 1
