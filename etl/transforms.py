"""Normalize raw events for storage."""

import hashlib
from datetime import datetime
from typing import Any


def normalize_event(raw_event: dict[str, Any]) -> dict[str, Any]:

    # Normalize source labels
    source = str(raw_event["source"]).strip().lower()
    source_series_id = str(raw_event["source_series_id"]).strip()

    # Parse canonical event date
    date_obj = datetime.strptime(str(raw_event["date"]), "%Y-%m-%d")
    category = str(raw_event["category"]).strip().lower()
    region = str(raw_event["region"]).strip().lower()
    normalized_series = str(raw_event["normalized_series"]).strip().lower()
    value = float(raw_event["value"])
    units = str(raw_event["units"]).strip().lower()
    frequency = str(raw_event["frequency"]).strip().lower()

    # Build unique record key to make upserts easier
    hash_input = f"{source}|{source_series_id}|{normalized_series}|{date_obj.date().isoformat()}|{value:.6f}"
    record_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

    return {
        "source": source,
        "source_series_id": source_series_id,
        "normalized_series": normalized_series,
        "category": category,
        "region": region,
        "date": date_obj.date().isoformat(),
        "year": int(raw_event["year"]),
        "month": int(raw_event["month"]),
        "value": value,
        "units": units,
        "frequency": frequency,
        "record_hash": record_hash,
    }
