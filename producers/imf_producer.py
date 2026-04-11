"""Publish IMF CPI events to Kafka."""

import csv
import json
import argparse
import logging
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from common.config import DATA_DIR, KAFKA_BOOTSTRAP_SERVERS
from common.series_mapping import IMF_SERIES_TO_METADATA

TOPIC = "cpi.raw.imf"


logger = logging.getLogger(__name__)


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )


def iter_events(
    row: dict[str, str], series_metadata: dict[str, str]
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for column_name, raw_value in row.items():
        # Keep monthly value columns
        if column_name[4:6] != "-M":
            continue
        if raw_value == "":
            continue

        year = int(column_name[:4])
        month = int(column_name[6:])
        events.append(
            {
                "source": "imf",
                "source_series_id": row["SERIES_CODE"].strip(),
                "date": f"{year:04d}-{month:02d}-01",
                "year": year,
                "month": month,
                "value": float(raw_value),
                "category": series_metadata["category"],
                "region": "us",
                "units": "index",
                "normalized_series": series_metadata["normalized_series"],
                "frequency": "monthly",
            }
        )
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="IMF CPI Kafka Producer")
    parser.add_argument("--file", required=True, help="The data file to process.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)

    csv_path = DATA_DIR / args.file
    producer = get_kafka_producer()
    sent_count = 0

    if not csv_path.exists():
        logger.error("File not found: %s", csv_path)
        return

    logger.info(
        "Starting producer source=%s topic=%s file=%s", "imf", TOPIC, csv_path.name
    )
    # Handle optional UTF-8 BOM
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            series_code = row["SERIES_CODE"].strip()
            series_metadata = IMF_SERIES_TO_METADATA.get(series_code)
            if series_metadata is None:
                continue

            for event in iter_events(row, series_metadata):
                event["ingested_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                message_key = (
                    f"{event['source']}:{event['source_series_id']}:{event['date']}"
                )
                producer.send(TOPIC, key=message_key, value=event)
                sent_count += 1
                logger.info(
                    "Published source=%s key=%s count=%s",
                    "imf",
                    message_key,
                    sent_count,
                )
                time.sleep(0.01)  # Slow replay slightly

    producer.flush()
    logger.info("Finished producer source=%s count=%s", "imf", sent_count)


if __name__ == "__main__":
    main()
