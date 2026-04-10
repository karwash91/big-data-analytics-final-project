"""Publish BLS CPI events to Kafka."""

import csv
import json
import argparse
import logging
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from common.config import DATA_DIR, KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)


TOPIC = "cpi.raw.bls"
DATA_FILE_PATTERN = "cu.data*"
TARGET_SERIES_ID = "CUSR0000SA0"
TARGET_NORMALIZED_SERIES = "us_all_items_cpi"


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )


def map_row(row: dict[str, str]) -> dict[str, object]:
    year = int(row["year"])
    # Strip leading period marker
    month = int(row["period"][1:])
    return {
        "source": "bls",
        "source_series_id": row["series_id"].strip(),
        "date": f"{year:04d}-{month:02d}-01",
        "year": year,
        "month": month,
        "value": float(row["value"]),
        "category": "all_items",
        "region": "us",
        "units": "index",
        "normalized_series": TARGET_NORMALIZED_SERIES,
        "frequency": "monthly",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="BLS CPI Kafka Producer")
    args = parser.parse_args([])  # No file argument needed

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)

    producer = get_kafka_producer()
    sent_count = 0

    # Find all files matching the pattern
    data_files = sorted(DATA_DIR.glob(DATA_FILE_PATTERN))
    if not data_files:
        logger.warning(
            "No data files found matching pattern=%s in directory=%s",
            DATA_FILE_PATTERN,
            DATA_DIR,
        )
        return

    logger.info(
        "Starting producer source=%s topic=%s files=%s",
        "bls",
        TOPIC,
        len(data_files),
    )

    for csv_path in data_files:
        logger.info(
            "Processing file source=%s topic=%s file=%s", "bls", TOPIC, csv_path.name
        )
        with csv_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            # Raw headers are padded
            reader.fieldnames = [field_name.strip() for field_name in reader.fieldnames]
            for row in reader:
                # Skip annual average row
                if row["period"].strip() == "M13":
                    continue
                if row["value"].strip() == "-":
                    logger.info(
                        "Skipping source=%s series=%s date=%s-%s reason=missing_value",
                        "bls",
                        row["series_id"].strip(),
                        row["year"].strip(),
                        row["period"].strip(),
                    )
                    continue

                event = map_row(row)
                # Stamp replay publish time
                event["ingested_at"] = (
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                )
                message_key = (
                    f"{event['source']}:{event['source_series_id']}:{event['date']}"
                )
                producer.send(TOPIC, key=message_key, value=event)
                sent_count += 1
                logger.info(
                    "Published source=%s key=%s count=%s", "bls", message_key, sent_count
                )
                time.sleep(0.01)  # Slow replay slightly
        
        # Remove file after consuming
        csv_path.unlink()
        logger.info("Removed file source=%s file=%s", "bls", csv_path.name)

    producer.flush()
    logger.info("Finished producer source=%s count=%s", "bls", sent_count)


if __name__ == "__main__":
    main()
