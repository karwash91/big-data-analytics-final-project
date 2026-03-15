"""Refresh analytics tables and export reports."""

import logging

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from common.config import (
    OUTPUTS_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    ROOT_DIR,
)

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    reports_dir = OUTPUTS_DIR.joinpath("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    # Use direct Postgres connection
    engine = create_engine(
        URL.create(
            drivername="postgresql+psycopg2",
            username=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
        )
    )

    # Refresh derived analytics tables
    refresh_comparison_metrics_sql = (
        ROOT_DIR.joinpath("analytics", "refresh_comparison_metrics.sql")
        .read_text(encoding="utf-8")
        .strip()[:-1]
    )

    refresh_derived_inflation_metrics_sql = (
        ROOT_DIR.joinpath("analytics", "refresh_derived_inflation_metrics.sql")
        .read_text(encoding="utf-8")
        .strip()[:-1]
    )

    refresh_queries = [
        refresh_comparison_metrics_sql,
        refresh_derived_inflation_metrics_sql,
    ]

    # Summarize latest source coverage
    summary_sql = """
        SELECT
            normalized_series,
            source,
            COUNT(*) AS record_count,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            ROUND(CAST(AVG(value) AS numeric), 4) AS avg_value
        FROM (
            -- Keep latest row per date
            SELECT DISTINCT ON (source, normalized_series, date)
                normalized_series,
                source,
                value,
                date
            FROM normalized_cpi
            ORDER BY source, normalized_series, date, created_at DESC, id DESC
        ) AS latest_normalized
        GROUP BY normalized_series, source
        ORDER BY normalized_series, source
    """

    # Surface largest source gaps
    divergence_sql = """
        SELECT
            date,
            normalized_series,
            source_a,
            source_b,
            absolute_difference,
            relative_difference
        FROM comparison_metrics
        ORDER BY absolute_difference DESC, date DESC
        LIMIT 10
    """

    logger.info("Starting analytics refresh")
    with engine.begin() as connection:
        # Rebuild analytics tables first
        for statement in refresh_queries:
            connection.execute(text(statement))

    # Export slide-ready report tables
    summary_df = pd.read_sql_query(summary_sql, engine)
    divergence_df = pd.read_sql_query(divergence_sql, engine)

    summary_df.to_csv(reports_dir.joinpath("source_summary.csv"), index=False)
    divergence_df.to_csv(reports_dir.joinpath("largest_divergence_periods.csv"), index=False)
    logger.info(
        "Wrote reports summary_rows=%s divergence_rows=%s",
        len(summary_df),
        len(divergence_df),
    )
    logger.info("Finished analytics refresh")
    engine.dispose()


if __name__ == "__main__":
    main()
