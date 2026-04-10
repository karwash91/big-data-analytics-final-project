# Streaming CPI Analytics Pipeline

This project implements the end-to-end CPI comparison pipeline using Python, Kafka, PostgreSQL, Docker Compose, exported analytics artifacts, and static charts.

## Repo structure

| Step | Folder | Purpose |
| - | - | - |
| 1 | data/ | Stores raw input files. |
| 2 | ui/ | Streamlit uploader that saves files to `data/` and runs the right producer. |
| 3 | producers/ | Reads source files and publishes Kafka events. |
| 4 | etl/ | Consumes Kafka events and loads database tables. |
| 5 | db/ | Defines PostgreSQL schema initialization SQL. |
| 6 | analytics/ | Refreshes derived tables and exports report files. |
| 7 | visualization/ | Builds PNG charts from database results. |
| 8 | outputs/ | Stores generated reports and charts. |
| - | common/ | Holds shared config values. |

## Quick start
This project is managed by Docker Compose and a Streamlit user interface.

**Pre-requisites**
- Install Python and Docker.
- Download raw data files:
  - **BLS:** https://download.bls.gov/pub/time.series/cu/cu.data.0.Current
  - **IMF:** https://data.imf.org/en/datasets/IMF.STA:CPI


0. Pre-requisites:
  - Install Python, Docker
  - Download the raw data files in your browser and place them in data/:
    - BLS: https://download.bls.gov/pub/time.series/cu/cu.data.0.Current
    - IMF: https://data.imf.org/en/datasets/IMF.STA:CPI
  - Note: Files can now have any name matching the pattern `cu.data*` for BLS data

docker compose down -v && docker compose up -d

1. Start infrastructure:

```bash
docker compose down -v
docker compose up -d
```

2. Open web interfaces:
  - Kafka UI: http://localhost:8080 (inspect topics and messages)
  - Streamlit UI: http://localhost:8501 (upload files and specify categories)

3. **Option A: Using Streamlit UI (Recommended)**
  - Open http://localhost:8501
  - Upload a BLS CPI data file
  - Enter a custom category name
  - Click "Load Data" to publish to Kafka
  - Proceed to step 4

4. **Option B: Manual file-based producer**
  - Place data files in the `data/` directory  
  - Run the producer manually:
  ```bash
  docker compose exec app python -m producers.bls_producer
  docker compose exec app python -m producers.imf_producer
  ```
  - Proceed to step 5

5. Start the ETL consumer (manual job, run once):
```bash
docker compose exec app python -m etl.consumer
```


6. Build analytics outputs:
```bash
docker compose exec app python -m analytics.analyze
docker compose exec app python -m visualization.build_charts
```


7. Review outputs:
  - charts in outputs/charts/
  - exported reports in outputs/reports/
  - Kafka UI in http://localhost:8080

    - Charts are saved in `outputs/charts/`
    - Exported reports are saved in `outputs/reports/`
    - You can inspect Kafka topics and messages via the Kafka UI at http://localhost:8080.
