# Streaming CPI Analytics Pipeline

A real-time data pipeline integrating BLS (Bureau of Labor Statistics) and IMF CPI data through Kafka streaming, PostgreSQL analytics, and automated reporting. Demonstrates end-to-end data ingestion, stream processing, normalized analytics, and visualization.

## Architecture

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Sources** | BLS, IMF CSV files | Raw CPI time series data |
| **Streaming** | Apache Kafka | Publish-subscribe message broker for data events |
| **Stream Processing** | Kafka Consumer (Python) | Subscribe to topics, parse messages, validate data |
| **Storage** | PostgreSQL | Raw, normalized, and derived analytics tables |
| **Analytics** | SQL refresh scripts | Generate growth rates, volatility, indexing, comparisons |
| **Reporting** | Python + Pandas | Export CSV reports, generate PNG charts |
| **Orchestration** | Docker Compose | Multi-container deployment with health checks |
| **Web UI** | Streamlit | File upload interface and Kafka monitoring |

## Project Structure

```
├── data/                          # Raw input files (cu.data.0.Current, IMF CSVs)
├── db/                            # PostgreSQL schema and migrations
│   ├── 00_init.sql               # Database initialization
│   ├── 01_raw_bls_cpi.sql        # Raw BLS table
│   ├── 02_raw_imf_cpi.sql        # Raw IMF table
│   ├── 03_normalized_cpi.sql     # Normalized multi-source table
│   ├── 04_comparison_metrics.sql # Source comparison analysis
│   ├── 05_derived_inflation_metrics.sql
│   ├── 06_idx_normalized_cpi_series_source.sql # Database index
│   └── 07_cpi_analysis.sql       # Analysis tables (growth, volatility, indexed)
├── producers/                     # Kafka producers
│   ├── bls_producer.py           # Publishes BLS CPI events
│   └── imf_producer.py           # Publishes IMF CPI events
├── etl/                           # Stream processing
│   ├── consumer.py               # Kafka consumer (auto-runs in Docker)
│   ├── loaders.py                # Database insert logic
│   └── transforms.py             # Data transformation utilities
├── analytics/                     # Analysis computation
│   ├── analyze.py                # Main analytics orchestrator
│   ├── refresh_cpi_growth_analysis.sql
│   ├── refresh_cpi_indexed_series.sql
│   ├── refresh_cpi_volatility.sql
│   ├── refresh_cpi_components_summary.sql
│   └── refresh_comparison_metrics.sql
├── visualization/                # Charting
│   └── build_charts.py           # Generate PNG visualizations
├── outputs/
│   ├── reports/                  # Exported CSV reports
│   └── charts/                   # Generated PNG charts
├── ui/                            # Web interfaces
│   └── streamlit_app.py          # Data upload UI
├── docker-compose.yml            # Container orchestration
├── Dockerfile                    # Application image
└── requirements.txt              # Python dependencies
```

## Quick Start Demo

### Prerequisites
- Python 3.12+ with venv
- Docker Desktop
- BLS data file: Download from https://download.bls.gov/pub/time.series/cu/cu.data.0.Current and save to `data/`

### Step 1: Start Infrastructure
```bash
docker compose up -d
```

Services start automatically:
- PostgreSQL (port 5432, ready to connect)
- Kafka broker (port 9092/9094)
- Kafka UI (port 8080)
- Streamlit UI (port 8501)
- ETL Consumer service (auto-running, processing messages)

### Step 2: Ingest Data
**Option A: Manual producer (fastest for demo)**
```bash
# Set up environment
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
python -m pip install -r requirements.txt

# Run producer (publishes all cu.data* files to Kafka)
python -m producers.bls_producer
# Wait for completion: will publish 1000+ messages
```

**Option B: Streamlit UI**
- Open http://localhost:8501
- Upload a BLS CPI data file
- Specify a category name
- Click "Load Data"

### Step 3: Monitor Stream Processing
Consumer automatically processes Kafka messages and stores to PostgreSQL:
- View Kafka topics: http://localhost:8080
- Check database: 
  ```bash
  docker exec cpi-postgres psql -U postgres -d cpi_analysis -c "SELECT COUNT(*) FROM normalized_cpi;"
  ```

### Step 4: Generate Analytics
```bash
python -m analytics.analyze
```

Automatically:
- Refreshes 4 analysis tables (cpi_growth_analysis, cpi_indexed_series, cpi_volatility, cpi_components_summary)
- Calculates growth rates (MoM, YoY, YTD)
- Computes volatility windows (30, 90, 365 days)
- Exports 5 CSV reports to `outputs/reports/`
- Generates 4 PNG charts to `outputs/charts/`

### Step 5: Review Results
```bash
# View latest data
docker exec cpi-postgres psql -U postgres -d cpi_analysis \
  -c "SELECT date, cpi_value, mom_growth_pct, yoy_growth_pct FROM cpi_growth_analysis ORDER BY date DESC LIMIT 5;"

# View chart files
ls -lh outputs/charts/
# View reports
ls -lh outputs/reports/
```

**Generated Artifacts:**

Charts (PNG):
- `bls_us_all_items_cpi.png` - CPI trend over 29 years
- `inflation_rate_by_source.png` - YoY growth rate comparison
- `source_divergence_over_time.png` - BLS vs IMF differences
- `imf_us_all_items_cpi.png` - IMF-sourced CPI trend

Reports (CSV):
- `cpi_growth_analysis.csv` - Growth metrics (MoM, YoY, YTD %)
- `cpi_indexed_series.csv` - Reindexed values (multiple base periods)
- `cpi_volatility_metrics.csv` - Rolling volatility (30, 90, 365 day windows)
- `source_summary.csv` - Data source statistics
- `largest_divergence_periods.csv` - Multi-source variance analysis

## Data Analysis Insights

### Current State (February 2026)
- **CPI Level:** 324.50 (vs 154.90 baseline in Jan 1997)
- **YoY Growth:** 2.56% (within Fed target range)
- **MoM Growth:** 0.25% (stable)

### Inflation Trend (349 months analyzed)
| Year | Avg YoY | Peak | Note |
|------|---------|------|------|
| 2008 | 4.19% | 7.20% | Financial crisis |
| 2020 | 0.91% | 2.72% | Pandemic deflation |
| 2021 | 4.80% | 7.32% | Recovery inflation begins |
| 2022 | 7.70% | 8.75% | **Peak inflation** |
| 2023 | 3.88% | 5.75% | Rapid cooling |
| 2024 | 3.06% | 3.68% | Continued decline |
| 2025 | 2.73% | 3.08% | Back to target |
| 2026 | 2.66% | 2.77% | Stable |

## Troubleshooting

**Consumer not processing messages:**
```bash
docker compose logs consumer | tail -50
```

**Database connection issues:**
```bash
docker compose exec postgres psql -U postgres -d cpi_analysis -c "\dt"
```

**Rebuild from scratch:**
```bash
docker compose down -v
rm -rf outputs/*
docker compose up -d
```
