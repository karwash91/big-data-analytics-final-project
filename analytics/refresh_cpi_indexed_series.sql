-- Refresh CPI indexed series (base period reindexing)
TRUNCATE TABLE cpi_indexed_series RESTART IDENTITY;

WITH latest_normalized AS (
    SELECT DISTINCT ON (source, normalized_series, date)
        source,
        normalized_series,
        category,
        date,
        value,
        EXTRACT(YEAR FROM date) as year
    FROM normalized_cpi
    ORDER BY
        source ASC, normalized_series ASC, date ASC, created_at DESC, id DESC
),
with_base_values AS (
    SELECT
        date,
        normalized_series,
        source,
        category,
        value as original_value,
        -- Find earliest value for 100-base
        FIRST_VALUE(value) OVER (PARTITION BY normalized_series, source ORDER BY date) as base_100_value,
        -- Find 2020 or earliest value
        COALESCE(
            (SELECT value FROM latest_normalized l2 
             WHERE l2.normalized_series = ln.normalized_series 
             AND l2.source = ln.source 
             AND l2.year = 2020 
             ORDER BY date LIMIT 1),
            FIRST_VALUE(value) OVER (PARTITION BY normalized_series, source ORDER BY date)
        ) as base_2020_value,
        -- Find 2015 or earliest value
        COALESCE(
            (SELECT value FROM latest_normalized l3 
             WHERE l3.normalized_series = ln.normalized_series 
             AND l3.source = ln.source 
             AND l3.year = 2015 
             ORDER BY date LIMIT 1),
            FIRST_VALUE(value) OVER (PARTITION BY normalized_series, source ORDER BY date)
        ) as base_2015_value
    FROM latest_normalized ln
)
INSERT INTO cpi_indexed_series (
    date,
    normalized_series,
    source,
    category,
    original_value,
    indexed_value_100_base,
    indexed_value_2020_base,
    indexed_value_2015_base
)
SELECT
    date,
    normalized_series,
    source,
    category,
    original_value,
    ROUND(
        CAST((original_value / NULLIF(base_100_value, 0)) * 100 AS numeric),
        4
    ) as indexed_value_100_base,
    ROUND(
        CAST((original_value / NULLIF(base_2020_value, 0)) * 100 AS numeric),
        4
    ) as indexed_value_2020_base,
    ROUND(
        CAST((original_value / NULLIF(base_2015_value, 0)) * 100 AS numeric),
        4
    ) as indexed_value_2015_base
FROM with_base_values
ORDER BY date ASC;
