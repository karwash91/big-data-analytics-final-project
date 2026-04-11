# Streaming CPI Analytics Pipeline

A small end-to-end demo that compares BLS and IMF CPI data through Kafka, PostgreSQL, and Streamlit. The UI keeps the demo narrow on purpose: **All items** as the main story, plus **Education** as the example of a narrower shared category.

## What the demo keeps

- Kafka producers and topics for raw events
- A consumer that loads raw and normalized rows into PostgreSQL
- One analytics table that matters for the demo:
  - `derived_inflation_metrics`
- A small chart set for two shared categories:
  - `bls_us_all_items_cpi.png`
  - `imf_us_all_items_cpi.png`
  - `all_items_yoy_inflation_by_source.png`
  - `bls_us_education_cpi.png`
  - `imf_us_education_cpi.png`
  - `education_yoy_inflation_by_source.png`

## Project structure

| Step | Folder | Purpose |
|------|--------|---------|
| 1 | `data/` | Raw BLS and IMF source files |
| 2 | `producers/` | Publish raw CPI events to Kafka |
| 3 | `etl/` | Consume Kafka topics and load PostgreSQL |
| 4 | `db/` | Raw, normalized, and demo analytics tables |
| 5 | `analytics/` | Refresh inflation metrics and export summary reports |
| 6 | `visualization/` | Build the demo PNG charts |
| 7 | `outputs/` | Exported CSV reports and chart images |
| 8 | `ui/` | Streamlit upload and demo page |
| - | `common/` | Shared config and series mapping |

## Demo mapping

The Streamlit UI supports two shared categories:

- `all_items`
  - BLS series: `CUSR0000SA0`
  - IMF series: `USA.CPI._T.IX.M`
  - Normalized series: `us_all_items_cpi`
- `education`
  - BLS series: `CUSR0000SAE1`
  - IMF series: `USA.CPI.CP10.IX.M`
  - Normalized series: `us_education_cpi`

## Quick start

### Prerequisites

- Python 3.12+
- Docker Desktop
- BLS CPI file such as `cu.data.0.Current`
- IMF CPI CSV export such as `dataset_*_IMF.STA_CPI_5.0.0.csv`

### 1. Start the stack

```bash
docker compose up -d
```

This starts PostgreSQL, Kafka, Kafka UI, the consumer, and Streamlit.

### 2. Load data in the UI

- Open `http://localhost:8501`
- Pick `All items` or `Education`
- Upload either the BLS file or the IMF file
- Click `Load Data`

The page publishes only the selected shared series for the uploaded source.

### 3. Monitor Kafka

- Streamlit UI: `http://localhost:8501`
- Kafka UI: `http://localhost:8080`

### 4. Refresh analytics manually if needed

```bash
python -m analytics.analyze
python -m visualization.build_charts
```

These commands refresh the demo analytics table and rebuild the demo charts.

### 5. Inspect results

```bash
# Normalized All items rows
docker exec cpi-postgres psql -U postgres -d cpi_analysis   -c "SELECT source, date, value FROM normalized_cpi WHERE normalized_series = 'us_all_items_cpi' ORDER BY date DESC LIMIT 5;"

# Normalized Education rows
docker exec cpi-postgres psql -U postgres -d cpi_analysis   -c "SELECT source, date, value FROM normalized_cpi WHERE normalized_series = 'us_education_cpi' ORDER BY date DESC LIMIT 5;"

# Inflation metrics
docker exec cpi-postgres psql -U postgres -d cpi_analysis   -c "SELECT source, date, pct_change_1m, pct_change_12m FROM derived_inflation_metrics WHERE normalized_series = 'us_education_cpi' ORDER BY date DESC LIMIT 5;"

# Exported artifacts
ls -lh outputs/reports/
ls -lh outputs/charts/
```

## Reports and charts

Reports:

- `source_summary.csv`
- `inflation_metrics.csv`

Charts:

- `bls_us_all_items_cpi.png`
- `imf_us_all_items_cpi.png`
- `all_items_yoy_inflation_by_source.png`
- `bls_us_education_cpi.png`
- `imf_us_education_cpi.png`
- `education_yoy_inflation_by_source.png`

## Notes

- The consumer stays separate from the UI so the Kafka boundary is still visible in the demo.
- The UI uses a small hardcoded mapping on purpose. That removes free-text category entry and keeps the story consistent.
- If you want a clean database after schema changes, reset the stack with `docker compose down -v` and start it again.
