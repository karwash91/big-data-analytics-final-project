import os
import subprocess
import time
from pathlib import Path

import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.config import (
    DATA_DIR,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from common.models import Base, IngestionAudit

DATA_DIR.mkdir(parents=True, exist_ok=True)

# --- Database Setup ---
DB_URI = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DB_URI)
Session = sessionmaker(bind=engine)
# Ensure the audit table exists on startup
Base.metadata.create_all(engine)

st.set_page_config(page_title="CPI Pipeline Data Uploader", page_icon="📈")

# Initialize session state for processed files
if "processed_files" not in st.session_state:
    st.session_state.processed_files = []

st.title("CPI Analytics Pipeline")
st.subheader("Data Uploader Drop Box")

st.markdown("""
Use the drop box below to upload your raw CPI data files (e.g., `cu.data.0.Current` and IMF datasets).
The files will be saved directly into the pipeline's `data/` directory where the producers can consume them.
""")

uploaded_files = st.file_uploader("Drag and drop files here", accept_multiple_files=True)

if uploaded_files:
    st.markdown("---")
    st.subheader("Uploaded Files Ready to Load")

    for uploaded_file in uploaded_files:
        # Save the file to the shared data volume
        file_path = DATA_DIR / uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        # Display file info and a "Load" button for each file
        is_processed = uploaded_file.name in st.session_state.processed_files
        col1, col2 = st.columns([3, 1])
        with col1:
            if is_processed:
                st.success(f"**Loaded:** `{uploaded_file.name}`")
            else:
                st.info(f"**File:** `{uploaded_file.name}`")

        with col2:
            if st.button(
                "Load",
                key=f"load_{uploaded_file.name}",
                use_container_width=True,
                disabled=is_processed,
            ):
                with Session() as session:
                    # 1. Check if the file has been processed before
                    existing_record = (
                        session.query(IngestionAudit)
                        .filter_by(filename=uploaded_file.name)
                        .first()
                    )
                    if existing_record:
                        st.warning(
                            f"File `{uploaded_file.name}` was already processed on "
                            f"{existing_record.ingested_at.strftime('%Y-%m-%d %H:%M')}. Skipping."
                        )
                        # Clean up the re-uploaded duplicate file to avoid clutter
                        try:
                            file_path.unlink(missing_ok=True)
                        except Exception as e:
                            st.error(f"Failed to remove duplicate file: {e}")
                        # Add to session state to remove from view and rerun
                        st.session_state.processed_files.append(uploaded_file.name)
                        time.sleep(3)
                        st.rerun()

                    # --- 2. File detection logic ---
                    producer_module = None
                    producer_name = None
                    if "cu.data" in uploaded_file.name:
                        producer_module = "producers.bls_producer"
                        producer_name = "BLS"
                    elif "IMF" in uploaded_file.name and uploaded_file.name.endswith(
                        ".csv"
                    ):
                        producer_module = "producers.imf_producer"
                        producer_name = "IMF"

                    if producer_module:
                        with st.spinner(
                            f"Running {producer_name} Producer for `{uploaded_file.name}`..."
                        ):
                            # --- 3. Run the producer ---
                            result = subprocess.run(
                                [
                                    "python",
                                    "-m",
                                    producer_module,
                                    "--file",
                                    uploaded_file.name,
                                ],
                                capture_output=True,
                                text=True,
                            )
                            if result.returncode == 0:
                                # --- 4. Audit and delete on success ---
                                audit_record = IngestionAudit(
                                    filename=uploaded_file.name
                                )
                                session.add(audit_record)
                                session.commit()
                                st.success(
                                    f"{producer_name} Producer finished successfully for `{uploaded_file.name}`!"
                                )
                                try:
                                    file_path.unlink(missing_ok=True)
                                    st.info(f"Removed processed file: `{uploaded_file.name}`.")
                                    # Add to session state and rerun to remove from UI
                                    st.session_state.processed_files.append(
                                        uploaded_file.name
                                    )
                                    time.sleep(3)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to remove file `{uploaded_file.name}`: {e}")
                            else:
                                st.error(
                                    f"Error processing `{uploaded_file.name}`:\n{result.stderr or result.stdout}"
                                )
                    else:
                        st.error(
                            f"Could not determine a producer for file: `{uploaded_file.name}`. Please check the file naming convention."
                        )