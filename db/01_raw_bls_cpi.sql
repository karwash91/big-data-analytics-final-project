-- Create raw BLS table
CREATE TABLE
IF NOT EXISTS raw_bls_cpi (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    source_series_id TEXT NOT NULL,
    category VARCHAR(100) NOT NULL,
    raw_payload_json JSONB NOT NULL,
    date DATE NOT NULL,
    value NUMERIC(12, 4) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_series_id, date, value)
);
