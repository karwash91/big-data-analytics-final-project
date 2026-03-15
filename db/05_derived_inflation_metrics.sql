-- Create derived inflation table
CREATE TABLE
IF NOT EXISTS derived_inflation_metrics (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    normalized_series TEXT NOT NULL,
    source TEXT NOT NULL,
    pct_change_1m NUMERIC(12, 4),
    pct_change_12m NUMERIC(12, 4),
    rolling_avg_3m NUMERIC(12, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, normalized_series, source)
);
