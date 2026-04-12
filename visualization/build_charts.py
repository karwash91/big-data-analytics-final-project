"""Build the small set of demo chart images."""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

from common.config import (
    OUTPUTS_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from common.series_mapping import CHART_FILENAMES_BY_CATEGORY, DEMO_CHART_FILENAMES, SHARED_CPI_SERIES

logger = logging.getLogger(__name__)


def get_db_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )


def read_sql_frame(connection, query: str, params: tuple[object, ...]) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


def remove_stale_demo_charts(output_dir: Path) -> None:
    for chart_path in output_dir.glob("*.png"):
        if chart_path.name not in DEMO_CHART_FILENAMES:
            chart_path.unlink()
    for filename in DEMO_CHART_FILENAMES:
        output_dir.joinpath(filename).unlink(missing_ok=True)


def plot_source_series(
    frame: pd.DataFrame,
    output_dir: Path,
    normalized_series: str,
    label: str,
) -> None:
    if frame.empty:
        return

    for source, group in frame.groupby("source"):
        output_path = output_dir.joinpath(f"{source}_{normalized_series}.png")
        plt.figure(figsize=(12, 6))
        plt.plot(group["date"], group["value"], linewidth=2)
        plt.title(f"{label} CPI Index - {source.upper()}")
        plt.xlabel("Date")
        plt.ylabel("CPI Index")
        plt.tight_layout()
        plt.savefig(output_path, dpi=160)
        plt.close()
        logger.info("Wrote chart %s", output_path.name)


def plot_yoy_inflation(
    frame: pd.DataFrame,
    output_dir: Path,
    category: str,
    label: str,
) -> None:
    if frame.empty:
        return

    output_path = output_dir.joinpath(f"{category}_yoy_inflation_by_source.png")
    plt.figure(figsize=(12, 6))
    for source, group in frame.groupby("source"):
        plt.plot(group["date"], group["pct_change_12m"], linewidth=2, label=source.upper())
    plt.title(f"{label} CPI YoY Inflation by Source")
    plt.xlabel("Date")
    plt.ylabel("Percent Change (12M)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()
    logger.info("Wrote chart %s", output_path.name)


def plot_top_inflation_periods(
    frame: pd.DataFrame,
    output_dir: Path,
    category: str,
    label: str,
) -> None:
    if frame.empty:
        return

    plot_frame = frame.sort_values(["pct_change_12m", "date"], ascending=[True, True])
    labels = plot_frame["date"].dt.strftime("%Y-%m") + " | " + plot_frame["source"].str.upper()
    output_path = output_dir.joinpath(f"{category}_top_12m_inflation_periods.png")

    plt.figure(figsize=(12, 6))
    plt.barh(labels, plot_frame["pct_change_12m"])
    plt.title(f"{label} Top 12-Month Inflation Periods")
    plt.xlabel("YoY Inflation (%)")
    plt.ylabel("Period | Source")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()
    logger.info("Wrote chart %s", output_path.name)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    output_dir = OUTPUTS_DIR.joinpath("charts")
    output_dir.mkdir(parents=True, exist_ok=True)
    remove_stale_demo_charts(output_dir)

    source_series_sql = """
        WITH latest_normalized AS (
            SELECT DISTINCT ON (source, normalized_series, date)
                source,
                normalized_series,
                date,
                value
            FROM normalized_cpi
            ORDER BY source, normalized_series, date, created_at DESC, id DESC
        )
        SELECT source, normalized_series, date, value
        FROM latest_normalized
        WHERE normalized_series = %s
        ORDER BY source, date
    """

    inflation_sql = """
        SELECT date, source, pct_change_12m
        FROM derived_inflation_metrics
        WHERE normalized_series = %s AND pct_change_12m IS NOT NULL
        ORDER BY date
    """

    top_inflation_sql = """
        SELECT
            date,
            source,
            pct_change_12m
        FROM derived_inflation_metrics
        WHERE normalized_series = %s AND pct_change_12m IS NOT NULL
        ORDER BY pct_change_12m DESC, date DESC, source ASC
        LIMIT 10
    """

    connection = get_db_connection()
    try:
        for category, series in SHARED_CPI_SERIES.items():
            normalized_series = series["normalized_series"]
            label = series["label"]
            logger.info(
                "Starting chart generation for category=%s normalized_series=%s",
                category,
                normalized_series,
            )

            params = (normalized_series,)
            source_series_frame = read_sql_frame(connection, source_series_sql, params)
            inflation_frame = read_sql_frame(connection, inflation_sql, params)
            top_inflation_frame = read_sql_frame(connection, top_inflation_sql, params)

            if not source_series_frame.empty:
                source_series_frame["date"] = pd.to_datetime(source_series_frame["date"])
            if not inflation_frame.empty:
                inflation_frame["date"] = pd.to_datetime(inflation_frame["date"])
            if not top_inflation_frame.empty:
                top_inflation_frame["date"] = pd.to_datetime(top_inflation_frame["date"])

            plot_source_series(source_series_frame, output_dir, normalized_series, label)
            plot_yoy_inflation(inflation_frame, output_dir, category, label)
            plot_top_inflation_periods(top_inflation_frame, output_dir, category, label)

        logger.info(
            "Finished chart generation for categories=%s",
            ",".join(CHART_FILENAMES_BY_CATEGORY),
        )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
