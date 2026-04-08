"""Streamlit UI for BLS CPI data ingestion."""

import csv
import json
import logging
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import streamlit as st
from kafka import KafkaProducer

from common.config import KAFKA_BOOTSTRAP_SERVERS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(page_title="BLS CPI Data Loader", layout="wide")
st.title("BLS CPI Data Ingestion")

# Initialize session state for status messages
if "status" not in st.session_state:
    st.session_state.status = None
if "status_type" not in st.session_state:
    st.session_state.status_type = None


def get_kafka_producer() -> KafkaProducer:
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )


def map_row(row: dict[str, str], category: str) -> dict[str, object]:
    """Transform a raw CSV row to a Kafka event."""
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
        "category": category,
        "region": "us",
        "units": "index",
        "normalized_series": category.lower().replace(" ", "_"),
        "frequency": "monthly",
    }


def process_file(uploaded_file, category: str) -> dict:
    """Process an uploaded file and publish to Kafka."""
    results = {
        "success": False,
        "sent_count": 0,
        "skipped_count": 0,
        "errors": [],
    }

    try:
        producer = get_kafka_producer()
        topic = "cpi.raw.bls"
        logger.info("Starting file processing file=%s topic=%s category=%s", uploaded_file.name, topic, category)

        # Create a temporary file from the upload
        with tempfile.NamedTemporaryFile(
            mode="w+b", suffix=".csv", delete=False
        ) as tmp:
            tmp.write(uploaded_file.getbuffer())
            tmp_path = Path(tmp.name)

        try:
            with tmp_path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                # Raw headers are padded
                reader.fieldnames = [
                    field_name.strip() for field_name in reader.fieldnames
                ]

                for row in reader:
                    # Skip annual average row
                    if row["period"].strip() == "M13":
                        results["skipped_count"] += 1
                        continue
                    if row["value"].strip() == "-":
                        results["skipped_count"] += 1
                        continue

                    try:
                        event = map_row(row, category)
                        # Stamp replay publish time
                        event["ingested_at"] = (
                            datetime.now(timezone.utc)
                            .replace(microsecond=0)
                            .isoformat()
                        )
                        message_key = f"{event['source']}:{event['source_series_id']}:{event['date']}"
                        producer.send(topic, key=message_key, value=event)
                        results["sent_count"] += 1
                        
                        if results["sent_count"] % 100 == 1:
                            logger.info("Published source=%s key=%s count=%s", event["source"], message_key, results["sent_count"])
                        time.sleep(0.001)  # Small delay between sends

                    except Exception as e:
                        error_msg = f"Row {results['sent_count'] + results['skipped_count']}: {str(e)}"
                        results["errors"].append(error_msg)
                        logger.error("Error processing row: %s", error_msg)

                producer.flush()
                results["success"] = True
                logger.info("Finished file processing sent_count=%s skipped_count=%s", results["sent_count"], results["skipped_count"])

        finally:
            # Clean up temp file
            tmp_path.unlink()

    except Exception as e:
        error_msg = f"Processing error: {str(e)}"
        results["errors"].append(error_msg)
        logger.error("Fatal processing error: %s", error_msg, exc_info=True)

    return results


# Split layout into columns
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Upload File")

    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a BLS CPI data file",
        help="Upload a file with the prefix 'cu.data' (e.g. cu.data.0.Current)",
    )

    # Category input
    category = st.text_input(
        "Enter Category Name",
        value="all_items",
        help="Category that will be stored for this data (e.g., 'all_items', 'energy', 'food')",
    )

    # Submit button
    if st.button("Load Data", type="primary", use_container_width=True):
        if uploaded_file is None:
            st.error("Please select a file first")
        elif not uploaded_file.name.startswith("cu.data"):
            st.error(f"Invalid file name: '{uploaded_file.name}'. File must start with 'cu.data' prefix.")
        elif not category.strip():
            st.error("Please enter a category name")
        else:
            with st.spinner("Processing file..."):
                results = process_file(uploaded_file, category.strip())

            if results["success"]:
                st.session_state.status = f"✓ Successfully sent {results['sent_count']} records to Kafka! (Skipped: {results['skipped_count']})"
                st.session_state.status_type = "success"
            else:
                st.session_state.status = f"✗ Error processing file: {', '.join(results['errors'][:3])}"
                st.session_state.status_type = "error"

with col2:
    st.subheader("Status")

    # Display status
    if st.session_state.status:
        if st.session_state.status_type == "success":
            st.success(st.session_state.status)
        else:
            st.error(st.session_state.status)

    # Display instructions
    st.info(
        """
        **How to use:**
        1. Select a BLS CPI data file (cu.data prefix, any format)
        2. Enter a category name for the data
        3. Click "Load Data" to publish to Kafka
        4. Run the ETL consumer separately to load data into the database
        
        **File Requirements:**
        - File name must start with "cu.data"
        - TSV delimiter (tab-separated)
        - Up to 2GB file size
        """
    )

# Footer
st.divider()
st.caption(
    "BLS CPI Data Ingestion UI - Data will be published to Kafka topic 'cpi.raw.bls'"
)
