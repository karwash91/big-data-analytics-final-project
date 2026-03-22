-- This script is run by the postgres container on first startup.
-- It creates the audit table for tracking ingested files.
-- The main `cpi_events` table is created by the ETL consumer via the ORM.

CREATE TABLE IF NOT EXISTS ingestion_audit (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL UNIQUE,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);