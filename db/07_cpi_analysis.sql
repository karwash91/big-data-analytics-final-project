-- Create CPI growth analysis table
CREATE TABLE IF NOT EXISTS cpi_growth_analysis (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    normalized_series TEXT NOT NULL,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    cpi_value NUMERIC(12, 4) NOT NULL,
    mom_growth NUMERIC(12, 4),
    mom_growth_pct NUMERIC(12, 4),
    yoy_growth NUMERIC(12, 4),
    yoy_growth_pct NUMERIC(12, 4),
    ytd_growth NUMERIC(12, 4),
    ytd_growth_pct NUMERIC(12, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, normalized_series, source)
);

-- Create CPI indexed series table (for base period reindexing)
CREATE TABLE IF NOT EXISTS cpi_indexed_series (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    normalized_series TEXT NOT NULL,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    original_value NUMERIC(12, 4) NOT NULL,
    indexed_value_100_base NUMERIC(12, 4),
    indexed_value_2020_base NUMERIC(12, 4),
    indexed_value_2015_base NUMERIC(12, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, normalized_series, source)
);

-- Create CPI components aggregate table
CREATE TABLE IF NOT EXISTS cpi_components_summary (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    component TEXT NOT NULL,
    series_count INTEGER,
    avg_value NUMERIC(12, 4),
    min_value NUMERIC(12, 4),
    max_value NUMERIC(12, 4),
    stddev_value NUMERIC(12, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, source, category, component)
);

-- Create CPI volatility metrics table
CREATE TABLE IF NOT EXISTS cpi_volatility (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    normalized_series TEXT NOT NULL,
    source TEXT NOT NULL,
    period_days INTEGER NOT NULL,
    volatility NUMERIC(12, 6),
    variance NUMERIC(12, 6),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, normalized_series, source, period_days)
);
