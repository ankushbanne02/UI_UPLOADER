import os
import re
import time
import base64
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
import streamlit.components.v1 as components
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
            border: 3px solid #000000;
            border-left: 8px solid #f58220;
            border-radius: 12px;
            padding: 18px;
            margin: 0 6px 14px 6px;
            transition: all 0.2s ease;
            box-shadow: 0 4px 12px rgba(0,0,0,0.18);
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
if "upload_success" not in st.session_state:
    st.session_state.upload_success = None  # dict with key/date/plc/start/end/count
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


# ---------------- SUCCESS DIALOG ----------------
if st.session_state.upload_success is not None:
    info = st.session_state.upload_success

    # Lock the dialog: hide the close (X) button, block ESC key, and block
    # backdrop clicks so the user can ONLY dismiss it via the action button.
    components.html(
        """
        <script>
        (function() {
            const doc = window.parent.document;
            const win = window.parent;

            if (!doc.getElementById('lock-dialog-css')) {
                const style = doc.createElement('style');
                style.id = 'lock-dialog-css';
                style.textContent = `
                    div[data-testid="stDialog"] button[aria-label="Close"],
                    div[role="dialog"] button[aria-label="Close"] {
                        display: none !important;
                    }
                `;
                doc.head.appendChild(style);
            }

            if (!win._dialogEscBlocked) {
                win._dialogEscBlocked = true;
                doc.addEventListener('keydown', function(e) {
                    if (e.key === 'Escape' &&
                        doc.querySelector('div[data-testid="stDialog"]')) {
                        e.stopPropagation();
                        e.preventDefault();
                    }
                }, true);
            }

            if (!win._dialogClickBlocked) {
                win._dialogClickBlocked = true;
                ['mousedown', 'click', 'pointerdown'].forEach(function(evt) {
                    doc.addEventListener(evt, function(e) {
                        const dialog = doc.querySelector('div[data-testid="stDialog"]');
                        if (!dialog) return;
                        const content = dialog.querySelector('div[role="dialog"]')
                            || dialog.firstElementChild;
                        if (content && !content.contains(e.target)) {
                            e.stopPropagation();
                            e.preventDefault();
                        }
                    }, true);
                });
            }
        })();
        </script>
        """,
        height=0,
    )

    @st.dialog("✅ Upload Successful")
    def _show_success_dialog():
        st.markdown(
            f"""
            <div style="padding: 8px 4px;">
                <div style="font-size:16px; color:#059669; font-weight:600; margin-bottom:14px;">
                    Logs uploaded to MongoDB successfully.
                </div>
                <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:10px; padding:14px;">
                    <div style="display:flex; justify-content:space-between; padding:6px 0; border-bottom:1px solid #f3f4f6;">
                        <span style="color:#6b7280;">Date</span>
                        <strong style="color:#111827;">{info['date']}</strong>
                    </div>
                    <div style="display:flex; justify-content:space-between; padding:6px 0; border-bottom:1px solid #f3f4f6;">
                        <span style="color:#6b7280;">PLC</span>
                        <strong style="color:#111827;">PLC {info['plc']}</strong>
                    </div>
                    <div style="display:flex; justify-content:space-between; padding:6px 0; border-bottom:1px solid #f3f4f6;">
                        <span style="color:#6b7280;">Start time</span>
                        <strong style="color:#059669;">{info['start']}</strong>
                    </div>
                    <div style="display:flex; justify-content:space-between; padding:6px 0; border-bottom:1px solid #f3f4f6;">
                        <span style="color:#6b7280;">End time</span>
                        <strong style="color:#dc2626;">{info['end']}</strong>
                    </div>
                    <div style="display:flex; justify-content:space-between; padding:6px 0;">
                        <span style="color:#6b7280;">Log lines</span>
                        <strong style="color:#111827;">{info['lines']}</strong>
                    </div>
                </div>
                <div style="margin-top:12px; color:#4b5563; font-size:13px;">{info['msg']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("OK, remove card", type="primary", use_container_width=True):
            st.session_state.data.pop(info["key"], None)
            if not st.session_state.data:
                st.session_state.filename = None
            st.session_state.upload_success = None
            st.rerun()

    _show_success_dialog()


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
        progress_bar = st.progress(0, text="Starting upload...")

        progress_bar.progress(5, text="Reading file... 5%")
        file_bytes = uploaded_file.getvalue()
        total_bytes = len(file_bytes) or 1

        progress_bar.progress(20, text=f"Decoding file... 20%")
        raw = file_bytes.decode("utf-8", errors="ignore")

        progress_bar.progress(35, text="Splitting into lines... 35%")
        lines = raw.splitlines(keepends=True)
        total_lines = len(lines) or 1

        # Parse lines incrementally so the bar moves from 35% -> 95%
        date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")
        plc_pattern = re.compile(r"PLC-(\d+)")
        grouped = defaultdict(list)
        step = max(total_lines // 50, 1)
        for idx, line in enumerate(lines):
            date_m = date_pattern.search(line)
            plc_m = plc_pattern.search(line)
            if date_m and plc_m:
                grouped[f"{date_m.group(1)}_PLC{plc_m.group(1)}"].append(line)
            if idx % step == 0:
                pct = 35 + int((idx / total_lines) * 60)
                progress_bar.progress(
                    min(pct, 95),
                    text=f"Parsing lines {idx + 1}/{total_lines}... {min(pct, 95)}%",
                )

        progress_bar.progress(100, text="Upload complete ✅ 100%")
        time.sleep(0.4)
        progress_bar.empty()

        st.session_state.data = dict(grouped)
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
                        # Stash details for the success dialog; do NOT remove card yet
                        st.session_state.upload_success = {
                            "key": key,
                            "date": date,
                            "plc": plc,
                            "start": start_t,
                            "end": end_t,
                            "lines": len(data[key]),
                            "msg": msg,
                        }
                        st.rerun()
                    else:
                        status_text.error(msg)
else:
    st.info("Upload a `.txt` log file above to get started.")
