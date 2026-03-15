-- Refresh comparison metrics
TRUNCATE TABLE comparison_metrics RESTART IDENTITY;

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

INSERT INTO comparison_metrics (
    date,
    normalized_series,
    source_a,
    source_b,
    value_a,
    value_b,
    absolute_difference,
    relative_difference
)
SELECT
    a.date,
    a.normalized_series,
    a.source AS source_a,
    b.source AS source_b,
    a.value AS value_a,
    b.value AS value_b,
    ABS(a.value - b.value) AS absolute_difference,
    CASE
        WHEN b.value = 0 THEN NULL
        ELSE ABS(a.value - b.value) / ABS(b.value)
    END AS relative_difference
FROM latest_normalized AS a
INNER JOIN latest_normalized AS b
    ON
        a.date = b.date
        AND a.normalized_series = b.normalized_series
        AND a.source < b.source;
