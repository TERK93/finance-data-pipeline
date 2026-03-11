# Finance Data Pipeline

End-to-end financial market data pipeline built with **Python + PostgreSQL** using a **Medallion Architecture (Landing → Bronze → Silver → Gold)**.

Features:
- Incremental ingestion from Yahoo Finance
- Data quality validation layer
- Dimensional modeling (`dim_ticker`)
- Analytics-ready gold views
- Modular ETL pipeline

---

## Architecture

![Pipeline Architecture](screenshots/architecture.png)

Pipeline implements a **Medallion Architecture** where data flows through
Landing → Bronze → Silver → Gold layers.


```
                    Yahoo Finance API
                           │
                           │ fetch_stocks.py
                           ▼
┌──────────────────────────────────────────┐
│ 🥉 LANDING                               │
│ table: landing_stock_prices              │
│ Raw OHLCV data (incremental load)        │
└──────────────────────┬───────────────────┘
                       │ load_bronze.py
                       ▼
┌──────────────────────────────────────────┐
│ 🥉 BRONZE                                │
│ table: bronze_stock_prices               │
│ Append-only history + load_date          │
└──────────────────────┬───────────────────┘
                       │ load_silver.py
                       ▼
┌──────────────────────────────────────────┐
│ 🥈 SILVER                                │
│ table: silver_stock_prices               │
│ Data validation + quality flags          │
└──────────────────────┬───────────────────┘
                       │ load_gold.py
                       ▼
┌──────────────────────────────────────────┐
│ 🥇 GOLD                                  │
│ views: gold_* (11 analytics views)       │
│ Returns · Moving averages · Volatility   │
└──────────────────────────────────────────┘

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python | Ingestion and pipeline logic |
| yfinance | Yahoo Finance API |
| pandas | Data manipulation |
| SQLAlchemy | Python → PostgreSQL connection |
| PostgreSQL | Database |
| DBeaver | Database GUI |
| dbt | Transformations (coming) |
| Apache Airflow | Orchestration (coming) |
| Power BI | Visualization (coming) |

---

## Data

**Tickers:** MSFT, GOOGL, AMZN, NVDA, META, SPOT, TSLA, JPM, V

**Fields:** date, ticker, open, high, low, close, volume

**History:** 2 years of daily OHLCV data

**Incremental load:** runs daily, fetches only new data

---

## Medallion Architecture

**Landing** —  Staging area. Replaced on every run, always reflects the latest API fetch. No history kept.     

**Bronze** — Copies from landing and adds load_date timestamp. Append-only, history preserved forever.

**Silver** — Reads from bronze, runs data quality checks:
- Null check on close and open
- Price validation (must be > 0)
- High must be ≥ low
- Volume must be positive
- Each row flagged with status = valid or error code

**Gold** — SQL views on top of silver. No data duplication.

---

## Key Design Decisions

- **Incremental load** — only new data is fetched each run, avoiding unnecessary API calls and duplicate data
- **Append-only bronze** — full history preserved forever with load_date for full auditability
- **Status flagging in silver** — invalid rows are flagged, not deleted, preserving data lineage
- **Gold as views** — no data duplication, always reflects latest silver data
- **Exclude current day** — avoids loading incomplete intraday data
- **Unique constraints on (ticker, date)** — prevents duplicates across all layers

--- 

## Gold Views

| View | Description |
|---|---|
| gold_daily_returns | Daily % return per ticker |
| gold_moving_average | 30 and 90 day moving averages |
| gold_performance_summary | YTD, 1M, 3M returns per ticker |
| gold_cumulative_returns | Cumulative return since start |
| gold_volatility | Rolling 30-day standard deviation |
| gold_drawdown | % drop from all-time high |
| gold_monthly_volume | Volume aggregated by month |
| gold_volume_vs_price | Volume signals vs price |
| gold_sector_comparison | Tech vs Finance vs Consumer |
| gold_correlation | Price correlation between tickers |
| gold_best_worst_performers | 30-day return ranking |

## Example Query

```sql
SELECT
    ticker,
    date,
    daily_return
FROM gold_daily_returns
WHERE ticker = 'NVDA'
ORDER BY date DESC
LIMIT 10;

---

## Screenshots

### Silver Layer — Validated OHLCV Data
![Silver Data](screenshots/silver_data.png)

### Gold Layer — Performance Summary
![Performance Summary](screenshots/gold_performance_summary.png)

### Gold Layer — Best & Worst Performers
![Best Worst Performers](screenshots/gold_best_worst_performers.png)

## Setup

**Prerequisites**
- Python 3.x
- PostgreSQL
- DBeaver (optional)

**Install dependencies**
```
pip install -r requirements.txt
```

**Configure environment**

Create a `.env` file in the root folder:
```
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=market_data_pipeline
``` 

**Run the pipeline**
```
python run_pipeline.py
```

Or run each step manually:
```
python fetch_stocks.py
python load_bronze.py
python load_silver.py
python load_dimensions.py
python load_gold.py
```

---

## Project Status

| Layer | Status |
|---|---|
| Landing | ✅ Done |
| Bronze | ✅ Done |
| Silver | ✅ Done |
| Gold views | ✅ Done |
| dim_ticker | ✅ Done |
| run_pipeline.py | ✅ Done |
| GitHub | 🔲 Coming |
| Power BI dashboard | 🔲 Coming |
| dbt models | 🔲 Coming |
| Airflow orchestration | 🔲 Coming |

---

## Roadmap

- [ ] Power BI dashboard connected to gold views
- [ ] dbt for silver/gold transformations
- [ ] Apache Airflow for daily scheduling
- [ ] Docker containerization
- [ ] Unit tests for quality checks

---

## Why This Project

Built to demonstrate end-to-end data engineering skills:
- Live API ingestion with incremental load logic
- Medallion architecture (landing → bronze → silver → gold)
- Data quality validation with status flagging
- Analytics-ready models for business intelligence
- Production-minded design with environment variables and single entrypoint