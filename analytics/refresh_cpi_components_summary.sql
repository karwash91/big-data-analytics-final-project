-- Refresh CPI components summary
TRUNCATE TABLE cpi_components_summary RESTART IDENTITY;

WITH latest_normalized AS (
    SELECT DISTINCT ON (source, normalized_series, date)
        source,
        normalized_series,
        category,
        date,
        value
    FROM normalized_cpi
    ORDER BY
        source ASC, normalized_series ASC, date ASC, created_at DESC, id DESC
)
INSERT INTO cpi_components_summary (
    date,
    source,
    category,
    component,
    series_count,
    avg_value,
    min_value,
    max_value,
    stddev_value
)
SELECT
    date,
    source,
    category,
    normalized_series as component,
    COUNT(*) as series_count,
    ROUND(CAST(AVG(value) AS numeric), 4) as avg_value,
    ROUND(CAST(MIN(value) AS numeric), 4) as min_value,
    ROUND(CAST(MAX(value) AS numeric), 4) as max_value,
    ROUND(CAST(STDDEV_POP(value) AS numeric), 4) as stddev_value
FROM latest_normalized
GROUP BY date, source, category, normalized_series
ORDER BY date ASC, source ASC, category ASC;
