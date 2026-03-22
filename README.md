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
This project is managed by Docker Compose and a Streamlit user interface.

1.  **Start all services:**

    ## Pre-requisites

  - Install Python and Docker.
  - Download the raw data files and place them in the `data/` directory:
  - **BLS:** https://download.bls.gov/pub/time.series/cu/cu.data.0.Current
  - **IMF:** https://data.imf.org/en/datasets/IMF.STA:CPI
  - Expected filenames in `data/`:
  - `cu.data.0.Current`
  - A `.csv` file from the IMF dataset download (e.g., `dataset_..._IMF.STA_CPI_5.0.0.csv`).

    ```bash
    docker compose down -v
    docker compose up -d
    ```
    This command will build the Docker images and start the PostgreSQL database, Kafka broker, a background ETL consumer, and the web UI.

2.  **Access the Uploader UI:**

    Open your web browser and navigate to [http://localhost:8501](http://localhost:8501).
    Kafka UI - [http://localhost:8080](http://localhost:8080).

3.  **Load Data:**

    Load files using "Browse files."  The UI will detect the files you placed in the `data/` directory. Click the "Load" button for each file to trigger the data producers. This sends the data into the Kafka pipeline, where the background consumer processes it into the database.

## Running Analytics

After loading data through the UI, you can manually trigger the analytics and chart-building scripts.

1.  **Build analytics outputs:**

    ```bash
    docker compose exec app python -m analytics.analyze
    docker compose exec app python -m visualization.build_charts
    ```

2.  **Review outputs:**

    - Charts are saved in `outputs/charts/`
    - Exported reports are saved in `outputs/reports/`
    - You can inspect Kafka topics and messages via the Kafka UI at http://localhost:8080.


