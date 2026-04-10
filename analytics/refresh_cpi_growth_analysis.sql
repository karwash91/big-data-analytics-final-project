-- Refresh CPI growth analysis
TRUNCATE TABLE cpi_growth_analysis RESTART IDENTITY;

WITH latest_normalized AS (
    SELECT DISTINCT ON (source, normalized_series, date)
        source,
        normalized_series,
        category,
        date,
        year,
        month,
        value
    FROM normalized_cpi
    ORDER BY
        source ASC, normalized_series ASC, date ASC, created_at DESC, id DESC
),
with_monthly_metrics AS (
    SELECT
        date,
        normalized_series,
        source,
        category,
        value as cpi_value,
        value - LAG(value) OVER (PARTITION BY normalized_series, source ORDER BY date) as mom_growth,
        ROUND(
            CAST(
                (
                    ((value / NULLIF(LAG(value) OVER (PARTITION BY normalized_series, source ORDER BY date), 0)) - 1) * 100
                ) AS numeric
            ),
            4
        ) as mom_growth_pct,
        value - LAG(value, 12) OVER (PARTITION BY normalized_series, source ORDER BY date) as yoy_growth,
        ROUND(
            CAST(
                (
                    ((value / NULLIF(LAG(value, 12) OVER (PARTITION BY normalized_series, source ORDER BY date), 0)) - 1) * 100
                ) AS numeric
            ),
            4
        ) as yoy_growth_pct,
        value - FIRST_VALUE(value) OVER (PARTITION BY normalized_series, source, EXTRACT(YEAR FROM date) ORDER BY date) as ytd_growth
    FROM latest_normalized
),
with_ytd_pct AS (
    SELECT
        *,
        ROUND(
            CAST(
                (
                    ((cpi_value / NULLIF(FIRST_VALUE(cpi_value) OVER (PARTITION BY normalized_series, source, EXTRACT(YEAR FROM date) ORDER BY date), 0)) - 1) * 100
                ) AS numeric
            ),
            4
        ) as ytd_growth_pct
    FROM with_monthly_metrics
)
INSERT INTO cpi_growth_analysis (
    date,
    normalized_series,
    source,
    category,
    cpi_value,
    mom_growth,
    mom_growth_pct,
    yoy_growth,
    yoy_growth_pct,
    ytd_growth,
    ytd_growth_pct
)
SELECT
    date,
    normalized_series,
    source,
    category,
    cpi_value,
    mom_growth,
    mom_growth_pct,
    yoy_growth,
    yoy_growth_pct,
    ytd_growth,
    ytd_growth_pct
FROM with_ytd_pct
ORDER BY date ASC;
