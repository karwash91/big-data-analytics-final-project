-- Create comparison metrics table
CREATE TABLE
IF NOT EXISTS comparison_metrics (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    normalized_series TEXT NOT NULL,
    source_a TEXT NOT NULL,
    source_b TEXT NOT NULL,
    value_a NUMERIC(12, 4) NOT NULL,
    value_b NUMERIC(12, 4) NOT NULL,
    absolute_difference NUMERIC(12, 4) NOT NULL,
    relative_difference NUMERIC(12, 6),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, normalized_series, source_a, source_b)
);
