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

    refresh_cpi_growth_analysis_sql = (
        ROOT_DIR.joinpath("analytics", "refresh_cpi_growth_analysis.sql")
        .read_text(encoding="utf-8")
        .strip()[:-1]
    )

    refresh_cpi_indexed_series_sql = (
        ROOT_DIR.joinpath("analytics", "refresh_cpi_indexed_series.sql")
        .read_text(encoding="utf-8")
        .strip()[:-1]
    )

    refresh_cpi_volatility_sql = (
        ROOT_DIR.joinpath("analytics", "refresh_cpi_volatility.sql")
        .read_text(encoding="utf-8")
        .strip()[:-1]
    )

    refresh_cpi_components_summary_sql = (
        ROOT_DIR.joinpath("analytics", "refresh_cpi_components_summary.sql")
        .read_text(encoding="utf-8")
        .strip()[:-1]
    )

    refresh_queries = [
        refresh_comparison_metrics_sql,
        refresh_derived_inflation_metrics_sql,
        refresh_cpi_growth_analysis_sql,
        refresh_cpi_indexed_series_sql,
        refresh_cpi_volatility_sql,
        refresh_cpi_components_summary_sql,
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

    # CPI growth analysis report
    cpi_growth_sql = """
        SELECT
            date,
            normalized_series,
            source,
            category,
            cpi_value,
            mom_growth_pct,
            yoy_growth_pct,
            ytd_growth_pct
        FROM cpi_growth_analysis
        WHERE yoy_growth_pct IS NOT NULL
        ORDER BY date DESC, source ASC
        LIMIT 50
    """

    # CPI volatility report
    volatility_sql = """
        SELECT
            date,
            normalized_series,
            source,
            period_days,
            volatility,
            variance
        FROM cpi_volatility
        WHERE period_days = 365
        ORDER BY date DESC, volatility DESC
        LIMIT 30
    """

    # CPI indexed series report
    indexed_cpi_sql = """
        SELECT
            date,
            normalized_series,
            source,
            original_value,
            indexed_value_100_base,
            indexed_value_2020_base,
            indexed_value_2015_base
        FROM cpi_indexed_series
        ORDER BY date DESC
        LIMIT 50
    """

    logger.info("Starting analytics refresh")
    with engine.begin() as connection:
        # Rebuild analytics tables first
        for statement in refresh_queries:
            connection.execute(text(statement))

    # Export slide-ready report tables
    summary_df = pd.read_sql_query(summary_sql, engine)
    divergence_df = pd.read_sql_query(divergence_sql, engine)
    cpi_growth_df = pd.read_sql_query(cpi_growth_sql, engine)
    volatility_df = pd.read_sql_query(volatility_sql, engine)
    indexed_cpi_df = pd.read_sql_query(indexed_cpi_sql, engine)

    summary_df.to_csv(reports_dir.joinpath("source_summary.csv"), index=False)
    divergence_df.to_csv(reports_dir.joinpath("largest_divergence_periods.csv"), index=False)
    cpi_growth_df.to_csv(reports_dir.joinpath("cpi_growth_analysis.csv"), index=False)
    volatility_df.to_csv(reports_dir.joinpath("cpi_volatility_metrics.csv"), index=False)
    indexed_cpi_df.to_csv(reports_dir.joinpath("cpi_indexed_series.csv"), index=False)

    logger.info(
        "Wrote reports summary_rows=%s divergence_rows=%s cpi_growth_rows=%s volatility_rows=%s indexed_rows=%s",
        len(summary_df),
        len(divergence_df),
        len(cpi_growth_df),
        len(volatility_df),
        len(indexed_cpi_df),
    )
    logger.info("Finished analytics refresh")
    engine.dispose()


if __name__ == "__main__":
    main()
