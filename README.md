# Azure Data Engineering Pipeline

> End-to-end data engineering portfolio project built on Azure + Databricks + Delta Lake.
> Architecture: Medallion (Bronze -> Silver -> Gold) | Governance: Unity Catalog | Orchestration: ADF

---

## Tech Stack

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat&logo=githubactions&logoColor=white)

| Component | Technology |
|-----------|-----------|
| Cloud | Azure |
| Processing | Databricks (PySpark) |
| Storage | ADLS Gen2 + Delta Lake |
| Orchestration | Azure Data Factory |
| Governance | Unity Catalog |
| CI/CD | GitHub Actions |

---

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full architecture diagram and decision rationale.

**Medallion Layers:**
- **Bronze** — Raw ingestion, append-only Delta tables with metadata columns
- **Silver** — Cleaned, typed, deduplicated data with schema enforcement
- **Gold** — Aggregated KPIs and business metrics ready for analytics

---

## Project Structure

```
azure-de-project/
+-- notebooks/
|   +-- bronze/      <- Raw ingestion notebooks
|   +-- silver/      <- Transformation notebooks
|   +-- gold/        <- Aggregation / KPI notebooks
+-- adf_pipelines/   <- ADF pipeline JSON exports
+-- unity_catalog/   <- Unity Catalog setup scripts
+-- ci_cd/
|   +-- .github/
|       +-- workflows/ <- GitHub Actions pipelines
+-- docs/
|   +-- architecture.md
+-- README.md
```

---

## Dataset

> To be updated after dataset selection.

---

## How to Run

> Setup instructions will be added as the project is built.

---

## Author

Data Engineering Bootcamp Student — Karachi, Pakistan
Building production-grade portfolio projects with Azure + Databricks.
