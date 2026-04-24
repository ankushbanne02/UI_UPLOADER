import os
import re
import time
import shutil
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


def _wipe_temp_folder():
    """Remove every file/sub-folder inside TEMP_FOLDER, then ensure it exists."""
    if os.path.isdir(TEMP_FOLDER):
        for entry in os.listdir(TEMP_FOLDER):
            entry_path = os.path.join(TEMP_FOLDER, entry)
            try:
                if os.path.isfile(entry_path) or os.path.islink(entry_path):
                    os.remove(entry_path)
                elif os.path.isdir(entry_path):
                    shutil.rmtree(entry_path, ignore_errors=True)
            except OSError:
                pass
    os.makedirs(TEMP_FOLDER, exist_ok=True)


# Ensure the folder exists; wipe leftovers from any previous run once at startup.
os.makedirs(TEMP_FOLDER, exist_ok=True)


# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="Parcel Log Uploader",
    page_icon="attached_assets/favicon_1777016208711.ico",
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
            position: relative;
            background: #ffffff;
            border: 3px solid #0f172a;
            border-radius: 16px;
            padding: 20px 20px 18px 22px;
            margin: 0 8px 18px 8px;
            transition: transform 0.25s ease, box-shadow 0.25s ease, border-color 0.25s ease;
            box-shadow: 0 6px 18px rgba(17, 24, 39, 0.10);
            overflow: hidden;
        }
        /* Subtle top corner glow */
        div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"]::after {
            content: "";
            position: absolute;
            top: -40px;
            right: -40px;
            width: 120px;
            height: 120px;
            background: radial-gradient(circle, rgba(245, 130, 32, 0.10) 0%, rgba(245, 130, 32, 0) 70%);
            pointer-events: none;
        }
        div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"]:hover {
            transform: translateY(-4px);
            border-color: #000000;
            box-shadow:
                0 4px 10px rgba(0, 0, 0, 0.10),
                0 16px 32px rgba(245, 130, 32, 0.20);
        }

        .card-date {
            display: inline-block;
            color: #b8530b;
            background: linear-gradient(90deg, #fff4e6 0%, #ffe7cc 100%);
            border: 1px solid #fcd9b4;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 1.2px;
            padding: 4px 10px;
            border-radius: 999px;
            margin-bottom: 10px;
        }
        .card-plc {
            color: #0f172a;
            font-size: 24px;
            font-weight: 800;
            letter-spacing: -0.3px;
            margin-bottom: 10px;
        }
        .card-meta {
            color: #4b5563;
            font-size: 13px;
            line-height: 1.7;
            background: #fafafa;
            border: 1px solid #f1f1f3;
            border-radius: 10px;
            padding: 10px 12px;
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

        /* Upload to MongoDB button — solid black with white text */
        div[class*="st-key-up_"] .stButton > button,
        .stElementContainer[class*="st-key-up_"] button {
            background: #000000 !important;
            color: #ffffff !important;
            border: none !important;
            border-radius: 8px !important;
            font-weight: 600 !important;
        }
        div[class*="st-key-up_"] .stButton > button p,
        .stElementContainer[class*="st-key-up_"] button p,
        div[class*="st-key-up_"] .stButton > button span,
        .stElementContainer[class*="st-key-up_"] button span {
            color: #ffffff !important;
        }
        div[class*="st-key-up_"] .stButton > button:hover,
        .stElementContainer[class*="st-key-up_"] button:hover {
            background: #1a1a1a !important;
            color: #ffffff !important;
            box-shadow: 0 4px 12px rgba(0,0,0,0.35) !important;
        }
        div[class*="st-key-up_"] .stButton > button:focus,
        div[class*="st-key-up_"] .stButton > button:active,
        .stElementContainer[class*="st-key-up_"] button:focus,
        .stElementContainer[class*="st-key-up_"] button:active {
            background: #000000 !important;
            color: #ffffff !important;
            border: none !important;
            box-shadow: 0 0 0 2px rgba(255,255,255,0.4) !important;
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
    """Returns (success: bool, message: str, records_uploaded: int)."""
    try:
        progress_cb(0.10, "Writing temp file...")
        temp_path = os.path.join(TEMP_FOLDER, f"{key}.txt")
        with open(temp_path, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line)

        progress_cb(0.30, "Converting TXT → JSON...")
        parsed_json = convert_txt_to_json(temp_path)
        if not parsed_json:
            return False, "No data parsed", 0

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
        return True, f"Uploaded {len(parsed_json)} records in {total} batch(es)", len(parsed_json)
    except Exception as e:
        return False, f"Error: {e}", 0


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
            padding-top: 0 !important;
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
            border-radius: 0;
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
                        <span style="color:#6b7280;">Records uploaded</span>
                        <strong style="color:#111827;">{info['records']}</strong>
                    </div>
                </div>
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
    # Inject JS that hooks into the browser's XHR upload to show real
    # local-to-server transfer progress (the step where the spinner shows).
    components.html(
        """
        <script>
        (function() {
            const win = window.parent;
            const doc = win.document;
            if (win._uploadXhrHooked) return;
            win._uploadXhrHooked = true;

            // Progress card placed right under the file uploader widget
            const overlay = doc.createElement('div');
            overlay.id = 'custom-upload-progress';
            overlay.style.cssText = `
                background: #ffffff; color: #111827;
                border: 2px solid #f58220;
                border-radius: 12px; padding: 12px 14px;
                box-shadow: 0 6px 18px rgba(245, 130, 32, 0.18);
                font-family: 'IBM Plex Sans', sans-serif;
                margin: 10px 0 4px 0; display: none;
            `;
            overlay.innerHTML = `
                <div id="cup-label" style="font-size:13px;font-weight:600;margin-bottom:8px;color:#111827;">
                    Uploading file... 0%
                </div>
                <div style="background:#f3f4f6;border-radius:8px;height:10px;overflow:hidden;">
                    <div id="cup-fill" style="background:linear-gradient(90deg,#f58220,#ff9a4d);height:100%;width:0%;transition:width 0.1s ease;"></div>
                </div>
                <div id="cup-meta" style="font-size:12px;color:#6b7280;margin-top:6px;">0 B / 0 B</div>
            `;

            // Try to mount it next to the uploader; retry until uploader exists.
            const mount = () => {
                const uploader = doc.querySelector('[data-testid="stFileUploader"]');
                if (uploader && !overlay.isConnected) {
                    uploader.parentNode.insertBefore(overlay, uploader.nextSibling);
                }
            };
            mount();
            const mo = new MutationObserver(mount);
            mo.observe(doc.body, { childList: true, subtree: true });

            const fmt = (n) => {
                const u = ['B','KB','MB','GB'];
                let i = 0;
                while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
                return n.toFixed(1) + ' ' + u[i];
            };

            const show = () => { overlay.style.display = 'block'; };
            const hide = () => {
                overlay.style.display = 'none';
                document.getElementById && (overlay.querySelector('#cup-fill').style.width = '0%');
            };
            const update = (loaded, total) => {
                const pct = total ? Math.min(Math.round((loaded / total) * 100), 100) : 0;
                overlay.querySelector('#cup-fill').style.width = pct + '%';
                overlay.querySelector('#cup-label').textContent =
                    'Uploading file to server... ' + pct + '%';
                overlay.querySelector('#cup-meta').textContent =
                    fmt(loaded) + ' / ' + fmt(total);
            };

            // Patch XHR
            const OrigXHR = win.XMLHttpRequest;
            function PatchedXHR() {
                const xhr = new OrigXHR();
                const origOpen = xhr.open;
                let isUpload = false;
                xhr.open = function(method, url) {
                    if (typeof url === 'string' &&
                        (url.includes('/_stcore/upload_file') ||
                         url.includes('upload_file'))) {
                        isUpload = true;
                    }
                    return origOpen.apply(xhr, arguments);
                };
                const origSend = xhr.send;
                xhr.send = function(body) {
                    if (isUpload && xhr.upload) {
                        show();
                        xhr.upload.addEventListener('progress', function(e) {
                            if (e.lengthComputable) update(e.loaded, e.total);
                        });
                        xhr.upload.addEventListener('load', function() {
                            update(1, 1);
                            setTimeout(hide, 600);
                        });
                        xhr.upload.addEventListener('error', hide);
                        xhr.upload.addEventListener('abort', hide);
                    }
                    return origSend.apply(xhr, arguments);
                };
                return xhr;
            }
            PatchedXHR.prototype = OrigXHR.prototype;
            win.XMLHttpRequest = PatchedXHR;

            // Also patch fetch in case Streamlit uses it
            const origFetch = win.fetch;
            win.fetch = function(input, init) {
                const url = typeof input === 'string' ? input : (input && input.url) || '';
                if (url.includes('upload_file') && init && init.body) {
                    show();
                    update(0, 1);
                    const p = origFetch.apply(this, arguments);
                    p.then(() => { update(1, 1); setTimeout(hide, 600); })
                     .catch(hide);
                    return p;
                }
                return origFetch.apply(this, arguments);
            };
        })();
        </script>
        """,
        height=0,
    )

    uploaded_file = st.file_uploader(
        "Upload a TXT log file",
        type=["txt"],
        accept_multiple_files=False,
        key=f"uploader_{st.session_state.uploader_key}",
        help="Upload one file. Clear all parsed groups before uploading another.",
    )

    if uploaded_file is not None:
        # Stream the uploaded file from memory to a temp file on the server
        # and show a 0% -> 100% progress bar based on bytes written.
        file_bytes = uploaded_file.getvalue()
        total_bytes = len(file_bytes) or 1

        def _human(n):
            for unit in ("B", "KB", "MB", "GB"):
                if n < 1024:
                    return f"{n:.1f} {unit}"
                n /= 1024
            return f"{n:.1f} TB"

        # Clear the temp folder before saving the newly uploaded file.
        _wipe_temp_folder()

        # Save the in-memory file to the server's temp folder.
        temp_upload_path = os.path.join(TEMP_FOLDER, uploaded_file.name)
        with open(temp_upload_path, "wb") as f:
            f.write(file_bytes)

        # Now parse the saved file
        with open(temp_upload_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
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
                with st.container(border=True):
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

                    success, msg, records = process_and_upload(key, data[key], cb)
                    if success:
                        # Stash details for the success dialog; do NOT remove card yet
                        st.session_state.upload_success = {
                            "key": key,
                            "date": date,
                            "plc": plc,
                            "start": start_t,
                            "end": end_t,
                            "records": records,
                            "msg": msg,
                        }
                        st.rerun()
                    else:
                        status_text.error(msg)
else:
    st.info("Upload a `.txt` log file above to get started.")
