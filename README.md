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

1. **Start all services**
```bash
docker compose down -v && docker compose up -d
```

Starts Postgres, Kafka, the always-on ETL consumer, and the Streamlit UI inside the Compose network.

2.  **Access the Uploader UI:**

    Open your web browser and navigate to:
    - [Streamlit UI](http://localhost:8501)
    - [Kafka UI](http://localhost:8080)

3.  **Load Data:**

    Load files using "Browse files."  The UI will detect the files you placed in the `data/` directory. Click the "Load" button for each file to trigger the data producers. This sends the data into the Kafka pipeline, where the background consumer processes it into the database.

## Running Analytics

After loading data through the UI, you can manually trigger the analytics and chart-building scripts.

4.  **Build analytics outputs:**

    ```bash
    docker compose exec app python -m analytics.analyze
    docker compose exec app python -m visualization.build_charts
    ```
5.  **Review outputs:**

    - Charts are saved in `outputs/charts/`
    - Exported reports are saved in `outputs/reports/`
    - You can inspect Kafka topics and messages via the Kafka UI at http://localhost:8080.
