-- Create normalized CPI index
CREATE INDEX IF NOT EXISTS idx_normalized_cpi_series_source ON normalized_cpi (
    normalized_series, source
);
