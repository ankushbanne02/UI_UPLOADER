# Parcel Log Uploader (Streamlit)

A modern Streamlit web app that replaces the original Tkinter desktop GUI in `app.py`. Same functionality, modern UI:

- Upload a `.txt` parcel log file
- Lines are parsed by date (`YYYY-MM-DD`) + PLC number (`PLC-N`) and grouped into keys like `2024-01-01_PLC3`
- Each group is shown as a glassmorphism card with Date, PLC, line count
- **Download Logs** — saves that group's lines as a `.txt`
- **Upload to MongoDB** — converts TXT → JSON via `KJ.convert_txt_to_json`, batches records (400/batch), inserts into MongoDB Atlas/Cosmos with duplicate-key tolerance, shows a progress bar
- After a successful upload the card is removed from the view

## Project structure

- `streamlit_app.py` — main Streamlit web app (entry point)
- `KJ.py` — TXT → JSON parser (unchanged from original)
- `app.py` — original Tkinter desktop app (kept for reference, not used)
- `.streamlit/config.toml` — Streamlit server config (port 5000, host 0.0.0.0, CORS/XSRF disabled for Replit proxy)

## Configuration

Mongo connection can be overridden with environment variables:
- `MONGO_URI` (default: the Cosmos URI from the original app)
- `DB_NAME` (default: `ASD`)
- `COLLECTION_NAME` (default: `parcels_data`)

## Run

The `Start application` workflow runs:
```
streamlit run streamlit_app.py
```
on port 5000 (webview).

## Deployment

Configured for `autoscale` deployment with the same `streamlit run` command.
