"""Insert raw and normalized records."""

from typing import Any

from psycopg2.extras import Json

SOURCE_TO_RAW_TABLE = {
    "bls": "raw_bls_cpi",
    "imf": "raw_imf_cpi",
}


def insert_raw_event(cursor: Any, raw_event: dict[str, Any]) -> None:
    # Route by source name
    table_name = SOURCE_TO_RAW_TABLE[raw_event["source"]]
    query = f"""
        INSERT INTO {table_name} (
            source,
            source_series_id,
            raw_payload_json,
            date,
            value
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source_series_id, date, value) DO NOTHING
    """
    cursor.execute(
        query,
        (
            raw_event["source"],
            raw_event["source_series_id"],
            # Store original event JSON
            Json(raw_event),
            raw_event["date"],
            raw_event["value"],
        ),
    )


def insert_normalized_event(cursor: Any, normalized_event: dict[str, Any]) -> None:
    cursor.execute(
        """
        INSERT INTO normalized_cpi (
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
            frequency,
            record_hash
        ) VALUES (
            %(source)s,
            %(source_series_id)s,
            %(normalized_series)s,
            %(category)s,
            %(region)s,
            %(date)s,
            %(year)s,
            %(month)s,
            %(value)s,
            %(units)s,
            %(frequency)s,
            %(record_hash)s
        )
        -- Upsert normalized duplicates
        ON CONFLICT (record_hash) DO UPDATE
        SET value = EXCLUDED.value,
            units = EXCLUDED.units,
            frequency = EXCLUDED.frequency
        """,
        normalized_event,
    )
