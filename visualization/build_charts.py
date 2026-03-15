"""Build chart images from analytics tables."""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from common.config import (
    OUTPUTS_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

logger = logging.getLogger(__name__)


def plot_source_series(frame: pd.DataFrame, output_dir: Path) -> None:
    """Plot raw source trends."""
    for (source, series_name), group in frame.groupby(["source", "normalized_series"]):
        output_path = output_dir.joinpath(f"{source}_{series_name}.png")
        plt.figure(figsize=(12, 6))
        plt.plot(group["date"], group["value"], marker="o")
        plt.title(f"{source.upper()} {series_name} Trend")
        plt.xlabel("Date")
        plt.ylabel("CPI Index")
        plt.tight_layout()
        plt.savefig(output_path, dpi=160)
        plt.close()
        logger.info("Wrote chart %s", output_path.name)


def plot_inflation(frame: pd.DataFrame, output_dir: Path) -> None:
    """Plot month-over-month inflation."""
    output_path = output_dir.joinpath("inflation_rate_by_source.png")
    plt.figure(figsize=(12, 6))
    for (series_name, source), group in frame.groupby(["normalized_series", "source"]):
        plt.plot(
            group["date"],
            group["pct_change_1m"],
            marker="o",
            label=f"{source} | {series_name}",
        )
    plt.title("Month-over-Month Inflation by Source")
    plt.xlabel("Date")
    plt.ylabel("Percent Change (1M)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()
    logger.info("Wrote chart %s", output_path.name)


def plot_divergence(frame: pd.DataFrame, output_dir: Path) -> None:
    """Plot source differences."""
    output_path = output_dir.joinpath("source_divergence_over_time.png")
    plt.figure(figsize=(12, 6))
    for (series_name, source_a, source_b), group in frame.groupby(
        ["normalized_series", "source_a", "source_b"]
    ):
        label = f"{source_a} vs {source_b} | {series_name}"
        plt.plot(group["date"], group["absolute_difference"], marker="o", label=label)
    plt.title("Absolute Divergence Between Sources")
    plt.xlabel("Date")
    plt.ylabel("Absolute Difference")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()
    logger.info("Wrote chart %s", output_path.name)


def main() -> None:
    """Query analytics tables and write chart files."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    output_dir = OUTPUTS_DIR.joinpath("charts")
    output_dir.mkdir(parents=True, exist_ok=True)
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

    inflation_sql = """
        SELECT
            date,
            normalized_series,
            source,
            pct_change_1m
        FROM derived_inflation_metrics
        ORDER BY date
    """

    source_series_sql = """
        SELECT
            source,
            normalized_series,
            date,
            value
        FROM (
            SELECT DISTINCT ON (source, normalized_series, date)
                source,
                normalized_series,
                date,
                value
            FROM normalized_cpi
            ORDER BY source, normalized_series, date, created_at DESC, id DESC
        ) AS latest_normalized
        ORDER BY source, normalized_series, date
    """

    divergence_sql = """
        SELECT
            date,
            normalized_series,
            source_a,
            source_b,
            absolute_difference
        FROM comparison_metrics
        ORDER BY date
    """

    logger.info("Starting chart generation")

    inflation_frame = pd.read_sql_query(inflation_sql, engine)
    source_series_frame = pd.read_sql_query(source_series_sql, engine)
    divergence_frame = pd.read_sql_query(divergence_sql, engine)
    inflation_frame["date"] = pd.to_datetime(inflation_frame["date"])
    source_series_frame["date"] = pd.to_datetime(source_series_frame["date"])
    divergence_frame["date"] = pd.to_datetime(divergence_frame["date"])

    plot_source_series(source_series_frame, output_dir)
    plot_inflation(inflation_frame, output_dir)
    plot_divergence(divergence_frame, output_dir)

    logger.info("Finished chart generation")
    engine.dispose()


if __name__ == "__main__":
    main()
