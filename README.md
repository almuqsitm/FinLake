# FinLake 🏛️

> **A production-style Financial Market Data Lakehouse Platform**

[![CI](https://github.com/YOUR_USERNAME/finlake/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/finlake/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11+-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.5-teal)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-black)

---

## Overview

FinLake is an end-to-end **Lakehouse data platform** for financial market data, demonstrating production-grade data engineering practices:

- 📥 **Batch + Streaming Ingestion** — Historical OHLCV data via Yahoo Finance + real-time tick events via Apache Kafka
- 🔄 **Medallion Architecture** — Bronze (raw) → Silver (cleansed) → Gold (curated) layers using Apache Iceberg
- 🕐 **Temporal Data Modeling** — SCD Type 2 for company dimension (tracks historical sector/metadata changes)
- ✅ **Data Quality Framework** — Completeness, range, and freshness checks + row-count reconciliation
- 🎯 **Orchestration** — Apache Airflow DAG with retry logic and dependency management
- 🚀 **CI/CD** — GitHub Actions pipeline (lint with ruff + pytest on every push)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│   DATA SOURCES                                                   │
│   yfinance API (batch OHLCV)  |  Simulated Ticker (Kafka)       │
└────────────┬────────────────────────────┬────────────────────────┘
             │ batch (JSON)               │ streaming (Avro)
             ▼                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER — Raw Zone (MinIO / S3)                           │
│  Partitioned JSON (batch) + Avro (streaming)                    │
│  Exact replica of source — no transformations                   │
└────────────┬────────────────────────────────────────────────────┘
             │ PySpark (Apache Spark 3.5)
             ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER — Cleansed Zone                                   │
│  Apache Iceberg tables (Parquet storage)                        │
│  Schema enforcement, deduplication, type casting                │
│  SCD Type 2: company_dim (tracks historical changes)            │
│  Data quality results table                                     │
└────────────┬────────────────────────────────────────────────────┘
             │ PySpark SQL
             ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER — Curated Analytics Zone                            │
│  Iceberg tables partitioned by symbol + date                    │
│  daily_ohlcv | rolling_metrics | portfolio_analytics            │
│  reconciliation_summary                                         │
└───────────────────────────────────────────────────────────────── ┘
                    ↑
        Airflow DAG orchestrates all layers
        GitHub Actions CI runs on every commit
```

---

## Tech Stack

| Category | Technology |
|---|---|
| Processing | Apache Spark 3.5 (PySpark) |
| Table Format | Apache Iceberg |
| Streaming | Apache Kafka + Avro |
| Storage | MinIO (S3-compatible) |
| Orchestration | Apache Airflow 2.9 |
| Data Quality | Custom DQ Framework + Great Expectations |
| CI/CD | GitHub Actions |
| Containerization | Docker Compose |
| Language | Python 3.11, SQL |
| Data Formats | JSON, Avro, Parquet |

---

## Quick Start

### Prerequisites
- Docker Desktop (>= 24.0)
- Python 3.11+
- 8GB+ RAM recommended (16GB for full stack)

### 1. Start Infrastructure
```bash
make docker-up
```
This starts Kafka, MinIO (S3-compatible storage), and Airflow. Once ready:
- **MinIO Console**: http://localhost:9001 (user: `minioadmin` / pass: `minioadmin`)
- **Airflow UI**: http://localhost:8080 (user: `admin` / pass: `admin`)

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Full Pipeline
```bash
make ingest-batch       # Bronze: batch OHLCV + company metadata
make ingest-stream      # Bronze: start Kafka tick producer (Ctrl+C to stop)
make transform-silver   # Silver: PySpark transform + SCD2
make transform-gold     # Gold: curated analytics tables
make run-dq             # Data quality checks
make reconcile          # Row-count reconciliation
```

### 4. Run Tests
```bash
make test
```

---

## Data Model

### Bronze Layer
| Dataset | Format | Partitioned By |
|---|---|---|
| `bronze/ohlcv/` | JSON | `symbol/date` |
| `bronze/company/` | JSON | `symbol` |
| `bronze/ticks/` | Avro | `symbol/hour` |

### Silver Layer (Iceberg)
| Table | Description |
|---|---|
| `silver.ohlcv` | Cleansed daily OHLCV, deduplicated |
| `silver.company_dim` | SCD Type 2 company dimension |
| `silver.dq_results` | Data quality check results |

### Gold Layer (Iceberg)
| Table | Description |
|---|---|
| `gold.daily_ohlcv` | Daily aggregated prices, partitioned by symbol+date |
| `gold.rolling_metrics` | 7d/30d volatility, RSI-14, avg volume |
| `gold.portfolio_analytics` | Cross-asset correlation and sector exposure |
| `gold.reconciliation_summary` | Pipeline health: row counts and reconciliation status |

---

## Key Design Decisions

### Why Apache Iceberg?
Iceberg gives us ACID transactions, **time travel** (query data as of any snapshot), and schema evolution on top of Parquet files. This is the table format used by modern Lakehouses at scale.

```sql
-- Time travel: query Gold data as it looked 7 days ago
SELECT * FROM gold.daily_ohlcv
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';
```

### Why SCD Type 2?
When a company changes sector (e.g., a tech company reclassified into financials), we don't overwrite — we close the old record and insert a new one. This preserves history, enabling point-in-time correct analytics.

### Why Reconciliation?
Row-count and checksum reconciliation between layers proves the pipeline is correct. At every layer boundary, we validate: `count(Bronze) == count(Silver) == count(Gold)`. Any break triggers an alert.

---

## Project Structure

```
finlake/
├── .github/workflows/ci.yml    # GitHub Actions CI
├── docker/docker-compose.yml   # Full local stack
├── src/
│   ├── ingestion/              # Bronze batch + streaming ingestors
│   ├── schemas/                # Avro + PySpark StructType schemas
│   ├── transformation/         # Bronze→Silver, SCD2, Silver→Gold
│   ├── quality/                # DQ checks + reconciliation
│   └── utils/                  # SparkSession factory, logger
├── dags/finlake_pipeline.py    # Airflow DAG
├── tests/                      # Unit tests (pytest + chispa)
├── notebooks/                  # Gold layer EDA + Iceberg time travel
└── docs/                       # Architecture + data dictionary
```

---

## CI/CD

Every push triggers:
1. `ruff check` — linting
2. `pytest tests/` — unit tests (run with local PySpark, no Docker required)

See [`.github/workflows/ci.yml`](.github/workflows/ci.yml)

---

## License
MIT
