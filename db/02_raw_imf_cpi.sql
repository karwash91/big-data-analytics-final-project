-- Create raw IMF table
CREATE TABLE
IF NOT EXISTS raw_imf_cpi (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    source_series_id TEXT NOT NULL,
    raw_payload_json JSONB NOT NULL,
    date DATE NOT NULL,
    value NUMERIC(12, 4) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_series_id, date, value)
);
