-- Refresh derived inflation metrics
TRUNCATE TABLE derived_inflation_metrics RESTART IDENTITY;

WITH latest_normalized AS (
    SELECT DISTINCT ON (source, normalized_series, date)
        source,
        source_series_id,
        normalized_series,
        category,
        region,
        date,
        year,
        month,
        value,
        units,
        frequency
    FROM normalized_cpi
    ORDER BY
        source ASC, normalized_series ASC, date ASC, created_at DESC, id DESC
)

INSERT INTO derived_inflation_metrics (
    date,
    normalized_series,
    source,
    pct_change_1m,
    pct_change_12m,
    rolling_avg_3m
)
SELECT
    date,
    normalized_series,
    source,
    ROUND(
        CAST(
            (
                ((value / NULLIF(LAG(value) OVER source_window, 0)) - 1) * 100
            ) AS numeric
        ),
        4
    ) AS pct_change_1m,
    ROUND(
        CAST(
            (
                ((value / NULLIF(LAG(value, 12) OVER source_window, 0)) - 1)
                * 100
            ) AS numeric
        ),
        4
    ) AS pct_change_12m,
    ROUND(
        CAST(
            AVG(value) OVER (
                PARTITION BY normalized_series, source
                ORDER BY date
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS numeric
        ),
        4
    ) AS rolling_avg_3m
FROM latest_normalized
WINDOW source_window AS (PARTITION BY normalized_series, source ORDER BY date);
