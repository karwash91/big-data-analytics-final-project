-- Create normalized CPI table
CREATE TABLE
IF NOT EXISTS normalized_cpi (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    source_series_id TEXT NOT NULL,
    normalized_series TEXT NOT NULL,
    category TEXT NOT NULL,
    region TEXT NOT NULL,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    value NUMERIC(12, 4) NOT NULL,
    units TEXT NOT NULL,
    frequency TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (record_hash)
);
