"""Refresh the small set of demo analytics tables and export reports."""

import logging
from pathlib import Path

import pandas as pd
import psycopg2

from common.config import (
    OUTPUTS_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    ROOT_DIR,
)
from common.series_mapping import SUPPORTED_NORMALIZED_SERIES

logger = logging.getLogger(__name__)
REPORT_FILENAMES = (
    "source_summary.csv",
    "inflation_metrics.csv",
)


def get_db_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )


def load_sql(filename: str) -> str:
    return ROOT_DIR.joinpath("analytics", filename).read_text(encoding="utf-8").strip()


def read_sql_frame(connection, query: str, params: tuple[object, ...]) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


def remove_stale_reports(reports_dir: Path) -> None:
    for report_path in reports_dir.glob("*.csv"):
        if report_path.name not in REPORT_FILENAMES:
            report_path.unlink()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    reports_dir = OUTPUTS_DIR.joinpath("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    remove_stale_reports(reports_dir)

    refresh_files = ("refresh_derived_inflation_metrics.sql",)

    summary_sql = """
        WITH latest_normalized AS (
            SELECT DISTINCT ON (source, normalized_series, date)
                normalized_series,
                source,
                value,
                date
            FROM normalized_cpi
            ORDER BY source, normalized_series, date, created_at DESC, id DESC
        )
        SELECT
            normalized_series,
            source,
            COUNT(*) AS record_count,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            ROUND(CAST(AVG(value) AS numeric), 4) AS avg_value
        FROM latest_normalized
        WHERE normalized_series = ANY(%s)
        GROUP BY normalized_series, source
        ORDER BY normalized_series, source
    """

    inflation_sql = """
        SELECT
            date,
            normalized_series,
            source,
            pct_change_1m,
            pct_change_12m,
            rolling_avg_3m
        FROM derived_inflation_metrics
        WHERE normalized_series = ANY(%s)
        ORDER BY normalized_series, date DESC, source ASC
    """

    logger.info(
        "Starting analytics refresh for normalized_series=%s",
        ",".join(SUPPORTED_NORMALIZED_SERIES),
    )
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            for filename in refresh_files:
                cursor.execute(load_sql(filename))
        connection.commit()

        params = (list(SUPPORTED_NORMALIZED_SERIES),)
        summary_df = read_sql_frame(connection, summary_sql, params)
        inflation_df = read_sql_frame(connection, inflation_sql, params)

        summary_df.to_csv(reports_dir.joinpath("source_summary.csv"), index=False)
        inflation_df.to_csv(reports_dir.joinpath("inflation_metrics.csv"), index=False)

        logger.info(
            "Wrote reports summary_rows=%s inflation_rows=%s",
            len(summary_df),
            len(inflation_df),
        )
        logger.info("Finished analytics refresh")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
