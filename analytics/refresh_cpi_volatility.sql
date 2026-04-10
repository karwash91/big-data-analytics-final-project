-- Refresh CPI volatility metrics
TRUNCATE TABLE cpi_volatility RESTART IDENTITY;

WITH latest_normalized AS (
    SELECT DISTINCT ON (source, normalized_series, date)
        source,
        normalized_series,
        date,
        value
    FROM normalized_cpi
    ORDER BY
        source ASC, normalized_series ASC, date ASC, created_at DESC, id DESC
),
volatility_30d AS (
    SELECT
        date,
        normalized_series,
        source,
        30 as period_days,
        STDDEV_POP(value) OVER (
            PARTITION BY normalized_series, source
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as volatility,
        VARIANCE(value) OVER (
            PARTITION BY normalized_series, source
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as variance
    FROM latest_normalized
),
volatility_90d AS (
    SELECT
        date,
        normalized_series,
        source,
        90 as period_days,
        STDDEV_POP(value) OVER (
            PARTITION BY normalized_series, source
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as volatility,
        VARIANCE(value) OVER (
            PARTITION BY normalized_series, source
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as variance
    FROM latest_normalized
),
volatility_365d AS (
    SELECT
        date,
        normalized_series,
        source,
        365 as period_days,
        STDDEV_POP(value) OVER (
            PARTITION BY normalized_series, source
            ORDER BY date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) as volatility,
        VARIANCE(value) OVER (
            PARTITION BY normalized_series, source
            ORDER BY date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) as variance
    FROM latest_normalized
)
INSERT INTO cpi_volatility (
    date,
    normalized_series,
    source,
    period_days,
    volatility,
    variance
)
SELECT date, normalized_series, source, period_days, 
       ROUND(CAST(volatility AS numeric), 6),
       ROUND(CAST(variance AS numeric), 6)
FROM volatility_30d
UNION ALL
SELECT date, normalized_series, source, period_days,
       ROUND(CAST(volatility AS numeric), 6),
       ROUND(CAST(variance AS numeric), 6)
FROM volatility_90d
UNION ALL
SELECT date, normalized_series, source, period_days,
       ROUND(CAST(volatility AS numeric), 6),
       ROUND(CAST(variance AS numeric), 6)
FROM volatility_365d
ORDER BY date ASC;
