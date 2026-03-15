# Streaming CPI Analytics Pipeline

This project implements the end-to-end CPI comparison pipeline using Python, Kafka, PostgreSQL, Docker Compose, exported analytics artifacts, and static charts.

## Repo structure

| Step | Folder | Purpose |
| - | - | - |
| 1 | data/ | Stores raw input files. |
| 2 | producers/ | Reads source files and publishes Kafka events. |
| 3 | etl/ | Consumes Kafka events and loads database tables. |
| 4 | db/ | Defines PostgreSQL schema initialization SQL. |
| 5 | analytics/ | Refreshes derived tables and exports report files. |
| 6 | visualization/ | Builds PNG charts from database results. |
| 7 | outputs/ | Stores generated reports and charts. |
| - | common/ | Holds shared config values. |

## Quick start

0. Pre-requisites:

- Install Python, Docker
- Download the raw data files in your browser and place them in data/:
  - BLS: https://download.bls.gov/pub/time.series/cu/cu.data.0.Current
  - IMF: https://data.imf.org/en/datasets/IMF.STA:CPI
- Expected filenames in data/:
  - cu.data.0.Current
  - dataset_..._IMF.STA_CPI_5.0.0.csv

1. Start infrastructure:

```bash
docker compose down -v
docker compose up -d
```

2. Open Kafka UI at http://localhost:8080 if you want to inspect topics and messages.

3. Start the ETL consumer in one terminal:

```bash
docker compose exec app python -m etl.consumer
```

4. Replay the datasets from another terminal:

```bash
docker compose exec app python -m producers.bls_producer
docker compose exec app python -m producers.imf_producer
```

5. Build analytics outputs:

```bash
docker compose exec app python -m analytics.analyze
docker compose exec app python -m visualization.build_charts
```

6. Review outputs:

- charts in outputs/charts/
- exported reports in outputs/reports/
- Kafka UI in http://localhost:8080
