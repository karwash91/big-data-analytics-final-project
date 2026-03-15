"""Consume raw topics into database tables."""

import json
import logging

import psycopg2
from kafka import KafkaConsumer

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from etl.loaders import insert_normalized_event, insert_raw_event
from etl.transforms import normalize_event

logger = logging.getLogger(__name__)

# Consume both raw topics
RAW_TOPICS = ["cpi.raw.bls", "cpi.raw.imf"]


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        *RAW_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        # Replay from topic start
        auto_offset_reset="earliest",
        # Commit offsets automatically
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)
    processed_count = 0
    connection = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )
    connection.autocommit = False
    consumer = build_consumer()

    logger.info("Starting ETL consumer topics=%s", ",".join(RAW_TOPICS))

    with connection.cursor() as cursor:

        for message in consumer:

            # Read raw Kafka event
            insert_raw_event(cursor, message.value)

            # Transform raw event fields
            normalized_event = normalize_event(message.value)

            # Load normalized database row
            insert_normalized_event(cursor, normalized_event)
            connection.commit()
            processed_count += 1

            logger.info(
                "Processed source=%s date=%s count=%s",
                normalized_event["source"],
                normalized_event["date"],
                processed_count,
            )


if __name__ == "__main__":
    main()
