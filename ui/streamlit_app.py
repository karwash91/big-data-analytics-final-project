"""Streamlit UI for the simplified CPI demo pipeline."""

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

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    OUTPUTS_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from common.series_mapping import (
    BLS_SERIES_TO_METADATA,
    CHART_FILENAMES_BY_CATEGORY,
    DEFAULT_CATEGORY_KEY,
    IMF_SERIES_TO_METADATA,
    SHARED_CPI_SERIES,
    SUPPORTED_CATEGORY_KEYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logging.getLogger("kafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

KAFKA_UI_URL = "http://localhost:8080"
BLS_TOPIC = "cpi.raw.bls"
IMF_TOPIC = "cpi.raw.imf"

st.set_page_config(page_title="Data Ingestion and Analytics Pipeline", layout="wide")
st.title("Data Ingestion and Analytics Pipeline")

if "status" not in st.session_state:
    st.session_state.status = None
if "status_type" not in st.session_state:
    st.session_state.status_type = None


def get_selected_series(category: str) -> dict[str, str]:
    return SHARED_CPI_SERIES[category]


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )


def get_db_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )


def map_bls_row(row: dict[str, str], category: str) -> dict[str, object]:
    series = get_selected_series(category)
    metadata = BLS_SERIES_TO_METADATA[series["bls_series_id"]]
    year = int(row["year"])
    month = int(row["period"][1:])
    return {
        "source": "bls",
        "source_series_id": row["series_id"].strip(),
        "date": f"{year:04d}-{month:02d}-01",
        "year": year,
        "month": month,
        "value": float(row["value"]),
        "category": metadata["category"],
        "region": "us",
        "units": "index",
        "normalized_series": metadata["normalized_series"],
        "frequency": "monthly",
    }


def iter_imf_events(row: dict[str, str], category: str) -> list[dict[str, object]]:
    series = get_selected_series(category)
    metadata = IMF_SERIES_TO_METADATA[series["imf_series_id"]]
    events: list[dict[str, object]] = []
    for column_name, raw_value in row.items():
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
                "category": metadata["category"],
                "region": "us",
                "units": "index",
                "normalized_series": metadata["normalized_series"],
                "frequency": "monthly",
            }
        )
    return events


def infer_uploaded_source(filename: str) -> str | None:
    if filename.startswith("cu.data"):
        return "bls"
    if "IMF" in filename and filename.endswith(".csv"):
        return "imf"
    return None


def get_normalized_record_count(source: str, normalized_series: str) -> int | None:
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM normalized_cpi
                    WHERE source = %s AND normalized_series = %s
                    """,
                    (source, normalized_series),
                )
                return int(cursor.fetchone()[0])
    except Exception as exc:
        logger.warning("Unable to count normalized rows: %s", exc)
        return None


def wait_for_consumer_load(
    source: str,
    normalized_series: str,
    baseline_count: int | None,
    timeout_seconds: int = 20,
) -> bool:
    if baseline_count is None:
        return False
    if baseline_count > 0:
        time.sleep(2)
        return True

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        current_count = get_normalized_record_count(source, normalized_series)
        if current_count is not None and current_count > baseline_count:
            return True
        time.sleep(1)
    return False


def refresh_analytics_outputs() -> tuple[bool, str]:
    try:
        from analytics.analyze import main as analyze_main
        from visualization.build_charts import main as build_charts_main

        analyze_main()
        build_charts_main()
        return True, "Analytics tables and chart images refreshed."
    except Exception as exc:
        logger.error("Failed to refresh analytics outputs: %s", exc, exc_info=True)
        return False, f"Analytics/chart refresh failed: {exc}"


def publish_event(
    producer: KafkaProducer,
    topic: str,
    event: dict[str, object],
    results: dict,
) -> None:
    event["ingested_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    message_key = f"{event['source']}:{event['source_series_id']}:{event['date']}"
    producer.send(topic, key=message_key, value=event)
    results["sent_count"] += 1
    time.sleep(0.001)


def process_bls_file(
    tmp_path: Path,
    producer: KafkaProducer,
    category: str,
    results: dict,
) -> None:
    series = get_selected_series(category)
    bls_series_id = series["bls_series_id"]
    logger.info(
        "Starting BLS file processing file=%s topic=%s category=%s bls_series_id=%s",
        tmp_path.name,
        BLS_TOPIC,
        category,
        bls_series_id,
    )

    with tmp_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        reader.fieldnames = [field_name.strip() for field_name in reader.fieldnames]

        for row in reader:
            if row["period"].strip() == "M13":
                results["skipped_count"] += 1
                continue
            if row["value"].strip() == "-":
                results["skipped_count"] += 1
                continue
            if row["series_id"].strip() != bls_series_id:
                continue

            try:
                publish_event(producer, BLS_TOPIC, map_bls_row(row, category), results)
            except Exception as exc:
                error_msg = f"Row {results['sent_count'] + results['skipped_count']}: {exc}"
                results["errors"].append(error_msg)
                logger.error("Error processing BLS row: %s", error_msg)

    if results["sent_count"] == 0:
        results["errors"].append(f"No rows found for mapped BLS series {bls_series_id}")


def process_imf_file(
    tmp_path: Path,
    producer: KafkaProducer,
    category: str,
    results: dict,
) -> None:
    series = get_selected_series(category)
    imf_series_id = series["imf_series_id"]
    logger.info(
        "Starting IMF file processing file=%s topic=%s category=%s imf_series_id=%s",
        tmp_path.name,
        IMF_TOPIC,
        category,
        imf_series_id,
    )

    with tmp_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["SERIES_CODE"].strip() != imf_series_id:
                continue

            for event in iter_imf_events(row, category):
                try:
                    publish_event(producer, IMF_TOPIC, event, results)
                except Exception as exc:
                    error_msg = f"Row {results['sent_count'] + results['skipped_count']}: {exc}"
                    results["errors"].append(error_msg)
                    logger.error("Error processing IMF row: %s", error_msg)
            break

    if results["sent_count"] == 0:
        results["errors"].append(f"No rows found for mapped IMF series {imf_series_id}")


def process_file(uploaded_file, category: str) -> dict:
    results = {
        "success": False,
        "sent_count": 0,
        "skipped_count": 0,
        "errors": [],
        "refresh_message": None,
    }

    try:
        source = infer_uploaded_source(uploaded_file.name)
        if source is None:
            results["errors"].append(f"Unsupported file name: '{uploaded_file.name}'")
            return results

        normalized_series = get_selected_series(category)["normalized_series"]
        baseline_count = get_normalized_record_count(source, normalized_series)
        producer = get_kafka_producer()
        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv", delete=False) as tmp:
            tmp.write(uploaded_file.getbuffer())
            tmp_path = Path(tmp.name)

        try:
            if source == "bls":
                process_bls_file(tmp_path, producer, category, results)
            else:
                process_imf_file(tmp_path, producer, category, results)

            producer.flush()
            results["success"] = results["sent_count"] > 0 and not results["errors"]
            if results["success"]:
                consumer_ready = wait_for_consumer_load(
                    source,
                    normalized_series,
                    baseline_count,
                )
                if consumer_ready:
                    refresh_success, refresh_message = refresh_analytics_outputs()
                    results["refresh_message"] = refresh_message
                    if not refresh_success:
                        results["errors"].append(refresh_message)
                        results["success"] = False
                else:
                    results["refresh_message"] = (
                        "Upload succeeded, but the consumer has not finished loading the new rows yet. "
                        "Refresh charts again in a few seconds."
                    )
            logger.info(
                "Finished file processing source=%s category=%s sent_count=%s skipped_count=%s",
                source,
                category,
                results["sent_count"],
                results["skipped_count"],
            )
        finally:
            tmp_path.unlink(missing_ok=True)

    except Exception as exc:
        error_msg = f"Processing error: {exc}"
        results["errors"].append(error_msg)
        logger.error("Fatal processing error: %s", error_msg, exc_info=True)

    return results


def get_chart_paths(category: str) -> list[Path]:
    charts_dir = OUTPUTS_DIR.joinpath("charts")
    if not charts_dir.exists():
        return []

    chart_paths = []
    for filename in CHART_FILENAMES_BY_CATEGORY[category]:
        chart_path = charts_dir.joinpath(filename)
        if chart_path.exists():
            chart_paths.append(chart_path)
    return chart_paths


def render_chart_image(chart_path: Path) -> None:
    image_bytes = chart_path.read_bytes()
    updated_at = datetime.fromtimestamp(chart_path.stat().st_mtime).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    caption = f"Updated {updated_at}"
    try:
        st.image(image_bytes, caption=caption, use_container_width=True)
    except TypeError:
        st.image(image_bytes, caption=caption, use_column_width=True)


def render_demo_resources(category: str) -> None:
    st.subheader("Resources")
    st.markdown(f"- [Open Kafka UI]({KAFKA_UI_URL})")
    st.markdown(f"- Charts directory: {OUTPUTS_DIR.joinpath('charts')}")
    if st.button("Refresh Analytics and Charts", use_container_width=True):
        with st.spinner("Refreshing analytics and charts..."):
            refresh_success, refresh_message = refresh_analytics_outputs()
        st.session_state.status = refresh_message
        st.session_state.status_type = "success" if refresh_success else "error"
        st.rerun()

    chart_paths = get_chart_paths(category)
    if not chart_paths:
        st.info(
            "No charts found yet."
        )
        return

    for chart_path in chart_paths:
        render_chart_image(chart_path)


selected_category = st.selectbox(
    "Series",
    options=SUPPORTED_CATEGORY_KEYS,
    index=SUPPORTED_CATEGORY_KEYS.index(DEFAULT_CATEGORY_KEY),
    format_func=lambda key: SHARED_CPI_SERIES[key]["label"],
)

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Upload File")
    st.info(
        "Upload a data file to publish records into the pipeline and refresh the demo outputs."
    )

    uploaded_file = st.file_uploader(
        "Choose data file",
        help="Upload a supported source file for the demo pipeline.",
    )

    if st.button("Load Data", type="primary", use_container_width=True):
        if uploaded_file is None:
            st.error("Please select a file first.")
        elif infer_uploaded_source(uploaded_file.name) is None:
            st.error("Unsupported file type.")
        else:
            with st.spinner("Processing file..."):
                results = process_file(uploaded_file, selected_category)

            if results["success"]:
                status = (
                    f"Successfully sent {results['sent_count']} records to Kafka "
                    f"(skipped: {results['skipped_count']})."
                )
                if results["refresh_message"]:
                    status = f"{status} {results['refresh_message']}"
                st.session_state.status = status
                st.session_state.status_type = "success"
            else:
                if results["errors"]:
                    st.session_state.status = f"Error processing file: {', '.join(results['errors'][:3])}"
                    st.session_state.status_type = "error"
                else:
                    st.session_state.status = (
                        results["refresh_message"]
                        or "Upload finished, but charts are not ready yet."
                    )
                    st.session_state.status_type = "warning"

with col2:
    st.subheader("Status")

    if st.session_state.status:
        if st.session_state.status_type == "success":
            st.success(st.session_state.status)
        elif st.session_state.status_type == "warning":
            st.warning(st.session_state.status)
        else:
            st.error(st.session_state.status)

    st.info(
        f"""
        **How to use:**
        1. Upload a supported data file
        2. Click Load Data
        3. The app publishes records to Kafka
        4. The running consumer loads the rows into PostgreSQL
        5. The app refreshes the analytics and charts
        """
    )

st.divider()
render_demo_resources(selected_category)
st.divider()
