import os
import re
import time
import base64
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
            background: #ffffff;
        }

        /* Hide default Streamlit chrome a bit */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}

        .parcel-card-head {
            padding: 4px 4px 0 4px;
        }

        /* Style each card's container */
        div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"] {
            background: #ffffff;
            border: 2px solid #000000;
            border-left: 5px solid #f58220;
            border-radius: 12px;
            padding: 18px;
            margin: 0 6px 14px 6px;
            transition: all 0.2s ease;
            box-shadow: 0 2px 6px rgba(0,0,0,0.06);
        }
        div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"]:hover {
            transform: translateY(-2px);
            border-color: #f58220;
            border-left-color: #f58220;
            box-shadow: 0 10px 24px rgba(245, 130, 32, 0.20);
        }

        .card-date {
            color: #f58220;
            font-size: 13px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 4px;
        }
        .card-plc {
            color: #111827;
            font-size: 22px;
            font-weight: 700;
            margin-bottom: 6px;
        }
        .card-meta {
            color: #4b5563;
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

        /* Uploader */
        [data-testid="stFileUploader"] {
            background: #fafafa;
            border: 2px dashed #f58220;
            border-radius: 14px;
            padding: 8px;
        }

        /* Default text color */
        .stMarkdown, label, p { color: #1f2937 !important; }
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


# ---------------- NAVBAR ----------------
def _img_b64(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("ascii")
    except Exception:
        return ""

VANDERLANDE_B64 = _img_b64("attached_assets/company_logo_1776967736368.jpeg")
LOGTALK_B64 = _img_b64("attached_assets/LogTalk_Logo_1776967736369.png")

st.markdown(
    f"""
    <style>
        header[data-testid="stHeader"] {{ display: none !important; }}
        div[data-testid="stToolbar"] {{ display: none !important; }}
        .block-container {{
            padding-top: 1rem !important;
            padding-left: 28px !important;
            padding-right: 28px !important;
            max-width: 100% !important;
        }}
        .navbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            background-color: #f58220;
            padding: 10px 20px;
            height: 60px;
            color: white;
            font-family: 'IBM Plex Sans', sans-serif;
            border-radius: 4px;
            margin: 0 -28px 20px -28px;
        }}
        .navbar .left, .navbar .right {{
            display: flex;
            align-items: center;
            background: white;
            padding: 4px 12px;
            border-radius: 6px;
        }}
        .navbar img {{ display: block; object-fit: contain; }}
        .navbar .left img {{ height: 40px; }}
        .navbar .right img {{ height: 46px; }}
    </style>
    <div class="navbar">
        <div class="left">
            <img src="data:image/jpeg;base64,{VANDERLANDE_B64}" alt="Vanderlande" />
        </div>
        <div class="right">
            <img src="data:image/png;base64,{LOGTALK_B64}" alt="LogTalk" />
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ---------------- UPLOADER ----------------
if not st.session_state.data:
    uploaded_file = st.file_uploader(
        "Upload a TXT log file",
        type=["txt"],
        accept_multiple_files=False,
        key=f"uploader_{st.session_state.uploader_key}",
        help="Upload one file. Clear all parsed groups before uploading another.",
    )

    if uploaded_file is not None:
        raw = uploaded_file.read().decode("utf-8", errors="ignore")
        lines = raw.splitlines(keepends=True)
        st.session_state.data = split_by_date_plc(lines)
        st.session_state.filename = uploaded_file.name
        # Reset the uploader so the file disappears from the upload box
        st.session_state.uploader_key += 1
        st.rerun()
else:
    st.info(
        f"📄 **{st.session_state.filename}** — clear all parsed groups below to upload a new file."
    )


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
                with st.container():
                    head_left, head_right = st.columns([5, 1])
                    with head_left:
                        st.markdown(
                            f"""
                            <div class="parcel-card-head">
                                <div class="card-date">{date}</div>
                                <div class="card-plc">PLC {plc}</div>
                                <div class="card-meta">
                                    <div><span style="color:#6b7280">Start:</span> <strong style="color:#059669">{start_t}</strong></div>
                                    <div><span style="color:#6b7280">End:</span> <strong style="color:#dc2626">{end_t}</strong></div>
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
                    with head_right:
                        if st.button("✕", key=f"close_{key}", help="Remove this card"):
                            st.session_state.data.pop(key, None)
                            if not st.session_state.data:
                                st.session_state.filename = None
                            st.rerun()

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
