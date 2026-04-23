import os
import re
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from KJ import convert_txt_to_json

# ---------------- CONFIG ----------------
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://logtalkdb:Admin_1316@db-logtalk-dev.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000",
)
DB_NAME = os.environ.get("DB_NAME", "ASD")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "parcels_data")
TEMP_FOLDER = "temp_logs"
BATCH_SIZE = 400
MAX_THREADS = 1

os.makedirs(TEMP_FOLDER, exist_ok=True)


# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="Parcel Log Uploader",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ---------------- STYLES ----------------
st.markdown(
    """
    <style>
        /* Page background */
        .stApp {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        }

        /* Hide default Streamlit chrome a bit */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}

        /* Header */
        .app-header {
            background: linear-gradient(90deg, #6366f1 0%, #8b5cf6 50%, #ec4899 100%);
            padding: 28px 32px;
            border-radius: 16px;
            margin-bottom: 24px;
            box-shadow: 0 10px 30px rgba(99, 102, 241, 0.3);
        }
        .app-header h1 {
            color: white;
            margin: 0;
            font-size: 32px;
            font-weight: 700;
            letter-spacing: -0.5px;
        }
        .app-header p {
            color: rgba(255,255,255,0.85);
            margin: 6px 0 0 0;
            font-size: 15px;
        }

        /* Card */
        .parcel-card {
            background: rgba(255, 255, 255, 0.06);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 14px;
            padding: 20px;
            margin-bottom: 16px;
            transition: all 0.2s ease;
        }
        .parcel-card:hover {
            transform: translateY(-2px);
            border-color: rgba(139, 92, 246, 0.5);
            box-shadow: 0 8px 24px rgba(139, 92, 246, 0.2);
        }
        .card-date {
            color: #a78bfa;
            font-size: 13px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 4px;
        }
        .card-plc {
            color: white;
            font-size: 22px;
            font-weight: 700;
            margin-bottom: 6px;
        }
        .card-meta {
            color: rgba(255,255,255,0.6);
            font-size: 13px;
            margin-bottom: 14px;
        }

        /* Buttons */
        .stButton > button {
            border-radius: 10px;
            border: none;
            font-weight: 600;
            padding: 8px 16px;
            transition: all 0.15s ease;
        }
        .stButton > button:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.25);
        }

        .stDownloadButton > button {
            background: linear-gradient(90deg, #06b6d4, #3b82f6);
            color: white;
            border-radius: 10px;
            border: none;
            font-weight: 600;
            width: 100%;
        }

        /* Stats row */
        .stat-card {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 12px;
            padding: 16px 20px;
            text-align: center;
        }
        .stat-num {
            color: #f9fafb;
            font-size: 28px;
            font-weight: 700;
        }
        .stat-label {
            color: rgba(255,255,255,0.6);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        /* Uploader */
        [data-testid="stFileUploader"] {
            background: rgba(255,255,255,0.04);
            border: 2px dashed rgba(139, 92, 246, 0.4);
            border-radius: 14px;
            padding: 8px;
        }

        /* Text white-ish */
        .stMarkdown, label, p { color: #e5e7eb !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------- STATE ----------------
if "data" not in st.session_state:
    st.session_state.data = {}  # key -> list[str]
if "filename" not in st.session_state:
    st.session_state.filename = None
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0
if "mongo_client" not in st.session_state:
    try:
        st.session_state.mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    except Exception:
        st.session_state.mongo_client = None


def get_collection():
    if st.session_state.mongo_client is None:
        st.session_state.mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return st.session_state.mongo_client[DB_NAME][COLLECTION_NAME]


# ---------------- LOGIC ----------------
def split_by_date_plc(lines):
    grouped = defaultdict(list)
    date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")
    for line in lines:
        date_m = date_pattern.search(line)
        plc_m = re.search(r"PLC-(\d+)", line)
        if not (date_m and plc_m):
            continue
        date = date_m.group(1)
        plc = plc_m.group(1)
        key = f"{date}_PLC{plc}"
        grouped[key].append(line)
    return dict(grouped)


def process_and_upload(key, lines, progress_cb):
    """Returns (success: bool, message: str)."""
    try:
        progress_cb(0.10, "Writing temp file...")
        temp_path = os.path.join(TEMP_FOLDER, f"{key}.txt")
        with open(temp_path, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line)

        progress_cb(0.30, "Converting TXT → JSON...")
        parsed_json = convert_txt_to_json(temp_path)
        if not parsed_json:
            return False, "No data parsed"

        for record in parsed_json:
            record["source_key"] = key

        progress_cb(0.45, "Preparing batches...")
        batches = [
            parsed_json[i:i + BATCH_SIZE]
            for i in range(0, len(parsed_json), BATCH_SIZE)
        ]
        total = len(batches)
        completed = 0

        collection = get_collection()

        def insert_batch(batch):
            try:
                collection.insert_many(batch, ordered=False)
            except BulkWriteError as e:
                for err in e.details.get("writeErrors", []):
                    if err.get("code") != 11000:
                        raise

        progress_cb(0.50, "Uploading to MongoDB...")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
            futures = [ex.submit(insert_batch, b) for b in batches]
            for _ in as_completed(futures):
                completed += 1
                pct = 0.50 + (completed / total) * 0.45
                progress_cb(pct, f"Uploading batch {completed}/{total}...")

        progress_cb(0.97, "Finalizing...")
        try:
            os.remove(temp_path)
        except OSError:
            pass

        progress_cb(1.0, "Upload complete ✅")
        return True, f"Uploaded {len(parsed_json)} records in {total} batch(es)"
    except Exception as e:
        return False, f"Error: {e}"


# ---------------- HEADER ----------------
st.markdown(
    """
    <div class="app-header">
        <h1>📦 Parcel Log Uploader</h1>
        <p>Logtalk · Upload, parse, and ship parcel logs to MongoDB</p>
    </div>
    """,
    unsafe_allow_html=True,
)


# ---------------- UPLOADER ----------------
uploaded_file = st.file_uploader(
    "Upload a TXT log file",
    type=["txt"],
    accept_multiple_files=False,
    key=f"uploader_{st.session_state.uploader_key}",
    help="Upload one file. After parsing, the uploader resets so you can add another.",
)

if uploaded_file is not None:
    raw = uploaded_file.read().decode("utf-8", errors="ignore")
    lines = raw.splitlines(keepends=True)
    st.session_state.data = split_by_date_plc(lines)
    st.session_state.filename = uploaded_file.name
    # Reset the uploader so the file disappears from the upload box
    st.session_state.uploader_key += 1
    st.rerun()

if st.session_state.filename:
    st.success(f"Loaded: {st.session_state.filename}")


# ---------------- STATS ----------------
data = st.session_state.data
if data:
    st.markdown("### Parsed groups")

    time_pattern = re.compile(r"(\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)")

    def get_time_range(lines):
        times = []
        for ln in lines:
            m = time_pattern.search(ln)
            if m:
                times.append(m.group(1))
        if not times:
            return "—", "—"
        return times[0], times[-1]

    # ---------------- CARDS GRID ----------------
    keys = sorted(data.keys())
    cols_per_row = 3
    for row_start in range(0, len(keys), cols_per_row):
        row_keys = keys[row_start:row_start + cols_per_row]
        cols = st.columns(cols_per_row)
        for col, key in zip(cols, row_keys):
            date, plc = key.split("_PLC")
            start_t, end_t = get_time_range(data[key])
            with col:
                st.markdown(
                    f"""
                    <div class="parcel-card">
                        <div class="card-date">{date}</div>
                        <div class="card-plc">PLC {plc}</div>
                        <div class="card-meta">
                            <div><span style="color:rgba(255,255,255,0.45)">Start:</span> <strong style="color:#a7f3d0">{start_t}</strong></div>
                            <div><span style="color:rgba(255,255,255,0.45)">End:</span> <strong style="color:#fca5a5">{end_t}</strong></div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                file_bytes = "".join(data[key]).encode("utf-8")
                st.download_button(
                    label="⬇ Download Logs",
                    data=file_bytes,
                    file_name=f"{date}_PLC{plc}.txt",
                    mime="text/plain",
                    key=f"dl_{key}",
                    use_container_width=True,
                )

                if st.button("⬆ Upload to MongoDB", key=f"up_{key}", use_container_width=True):
                    progress_bar = st.progress(0.0)
                    status_text = st.empty()

                    def cb(pct, msg, _bar=progress_bar, _txt=status_text):
                        _bar.progress(min(max(pct, 0.0), 1.0))
                        _txt.info(msg)

                    success, msg = process_and_upload(key, data[key], cb)
                    if success:
                        status_text.success(msg)
                        time.sleep(0.8)
                        # Remove the card after successful upload
                        st.session_state.data.pop(key, None)
                        st.rerun()
                    else:
                        status_text.error(msg)
else:
    st.info("Upload a `.txt` log file above to get started.")
