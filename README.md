# Streaming CPI Analytics Pipeline

A real-time data pipeline integrating BLS (Bureau of Labor Statistics) and IMF CPI data through Kafka streaming, PostgreSQL analytics, and automated reporting. Demonstrates end-to-end data ingestion, stream processing, normalized analytics, and visualization.

## Architecture & Project Structure

| Step | Folder | Technology | Purpose |
|------|--------|-----------|---------|
| 1 | data/ | CSV files | Raw CPI time series data (BLS, IMF) |
| 2 | producers/ | Python + Kafka | Publish BLS/IMF events to Kafka topics |
| 3 | etl/ | Kafka Consumer | Subscribe to topics, parse, validate, load to database |
| 4 | db/ | PostgreSQL | Raw, normalized, and derived analytics tables |
| 5 | analytics/ | SQL + Python | Refresh analysis tables; calculate growth, volatility, indexing |
| 6 | visualization/ | Matplotlib | Generate PNG charts from database results |
| 7 | outputs/ | CSV + PNG | Exported reports and generated charts |
| 8 | ui/ | Streamlit | Web interface for file upload and Kafka UI |
| - | common/ | Python | Shared configuration and utilities |
| - | docker-compose.yml | Docker Compose | Multi-container orchestration (Kafka, PostgreSQL, services) |

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
