

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import re
from collections import defaultdict
from pymongo import MongoClient
from KJ import convert_txt_to_json
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo.errors import BulkWriteError


# ---------------- CONFIG ----------------
MONGO_URI = "mongodb+srv://logtalkdb:Admin_1316@db-logtalk-dev.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
# MONGO_URI ="mongodb://localhost:27017"
DB_NAME = "ASD"
COLLECTION_NAME = "parcels_data"
TEMP_FOLDER = "temp_logs"
# ---------------------------------------

os.makedirs(TEMP_FOLDER, exist_ok=True)


class ParcelUploaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("📦 Parcel Log Uploader")
        self.root.geometry("1000x500")

        tk.Label(root, text="Logtalk : Parcel Log Uploader",
                 font=("Arial", 18, "bold")).pack(pady=10)

        tk.Button(root, text="Upload TXT File",
                  font=("Arial", 12),
                  command=self.upload_file).pack(pady=10)

        self.info = tk.Label(root, text="", fg="blue")
        self.info.pack()

        self.frame = tk.Frame(root)
        self.frame.pack(fill="both", expand=True)

        self.data = defaultdict(list)
        self.cards = {}

        self.mongo_client = MongoClient(MONGO_URI)
        self.collection = self.mongo_client[DB_NAME][COLLECTION_NAME]

    # ---------------- UPLOAD ----------------
    def upload_file(self):
        path = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt")])
        if not path:
            return

        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        self.split_by_date_plc(lines)
        self.show_cards()

        self.info.config(text=f"Loaded: {os.path.basename(path)}")

    # ---------------- SPLIT ----------------
    def split_by_date_plc(self, lines):
        self.data.clear()
        date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

        for line in lines:
            date_m = date_pattern.search(line)
            plc_m = re.search(r'PLC-(\d+)', line)

            if not (date_m and plc_m):
                continue

            date = date_m.group(1)
            plc = plc_m.group(1)

            key = f"{date}_PLC{plc}"
            self.data[key].append(line)

    # ---------------- UI ----------------
    def show_cards(self):
        for w in self.frame.winfo_children():
            w.destroy()

        self.cards.clear()

        col = 0
        for key in sorted(self.data):
            self.make_card(key, col)
            col += 1

    def make_card(self, key, col):
        card = tk.Frame(self.frame, bd=2, relief="ridge", padx=12, pady=12)
        card.grid(row=0, column=col, padx=10, pady=10, sticky="n")

        date, plc = key.split("_PLC")

        tk.Label(card, text=f"Date: {date}", font=("Arial", 12, "bold")).pack(anchor="w")
        tk.Label(card, text=f"PLC: {plc}", font=("Arial", 11)).pack(anchor="w")

        tk.Button(card, text="⬇ Download Logs",
                  command=lambda k=key: self.download_logs(k)).pack(pady=4)

        tk.Button(card, text="⬆ Upload Logs",
                  command=lambda k=key: self.start_upload(k)).pack(pady=4)

        status = tk.Label(card, text="", fg="gray")
        status.pack()

        self.cards[key] = (card, status)

    # ---------------- DOWNLOAD ----------------
    def download_logs(self, key):
        date, plc = key.split("_PLC")

        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=f"{date}_PLC{plc}.txt",
            filetypes=[("Text Files", "*.txt")]
        )

        if not path:
            return

        with open(path, "w", encoding="utf-8") as f:
            for line in self.data[key]:
                f.write(line)

        messagebox.showinfo("Saved", "Logs downloaded successfully")

    # ---------------- PROGRESS POPUP ----------------
    def show_progress_popup(self):
        popup = tk.Toplevel(self.root)
        popup.title("Uploading...")
        popup.geometry("350x150")
        popup.resizable(False, False)

        popup.transient(self.root)
        popup.grab_set()

        tk.Label(popup, text="Uploading Logs",
                 font=("Arial", 12, "bold")).pack(pady=10)

        status_label = tk.Label(popup, text="Starting...", fg="blue")
        status_label.pack()

        bar = ttk.Progressbar(popup, orient="horizontal",
                              length=280, mode="determinate")
        bar.pack(pady=15)
        bar["maximum"] = 100

        return popup, status_label, bar

    # ---------------- UPLOAD ----------------
    def start_upload(self, key):
        popup, status_label, bar = self.show_progress_popup()

        threading.Thread(
            target=self.process_and_upload,
            args=(key, popup, status_label, bar),
            daemon=True
        ).start()

    



    def process_and_upload(self, key, popup, status_label, bar):
        try:
            def ui(text, value):
                self.root.after(0, lambda: status_label.config(text=text))
                self.root.after(0, lambda: bar.config(value=value))

        # ---------------- STEP 1 ----------------
            ui("Writing temp file...", 10)

            temp_path = os.path.join(TEMP_FOLDER, f"{key}.txt")
            with open(temp_path, "w", encoding="utf-8") as f:
                for line in self.data[key]:
                    f.write(line)

        # ---------------- STEP 2 ----------------
            ui("Converting TXT → JSON...", 30)

            parsed_json = convert_txt_to_json(temp_path)

            if not parsed_json:
                ui("No data parsed", 0)
                return

            for record in parsed_json:
                record["source_key"] = key

        # ---------------- STEP 3 ----------------
            ui("Preparing batches...", 45)

            BATCH_SIZE = 400
            MAX_THREADS = 1

            batches = [
            parsed_json[i:i + BATCH_SIZE]
            for i in range(0, len(parsed_json), BATCH_SIZE)
        ]

            total_batches = len(batches)
            completed = 0


            

        # # ---------------- STEP 4 ----------------
        #     ui("Uploading to MongoDB...", 50)

        #     def insert_batch(batch):
        #         self.collection.insert_many(batch, ordered=False)

        #     with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        #         futures = [executor.submit(insert_batch, b) for b in batches]

        #         for future in as_completed(futures):
        #             completed += 1
        #             progress = 50 + int((completed / total_batches) * 45)
        #             ui(f"Uploading batch {completed}/{total_batches}...", progress)


         # ---------------- STEP 4 ----------------
            ui("Uploading to MongoDB...", 50)

            def insert_batch(batch):
                try:
                    self.collection.insert_many(batch, ordered=False)
                except BulkWriteError as e:
                # Ignore duplicate key errors (code 11000)
                    for err in e.details.get("writeErrors", []):
                        if err.get("code") != 11000:
                            raise  # re-raise any other errors

        # Parallel batch upload
            with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                futures = [executor.submit(insert_batch, b) for b in batches]
                for future in as_completed(futures):
                    completed += 1
                    progress = 50 + int((completed / total_batches) * 45)
                    ui(f"Uploading batch {completed}/{total_batches}...", progress)

        # ---------------- STEP 5 ----------------
            ui("Finalizing...", 95)

            os.remove(temp_path)

            ui("Upload Complete ", 100)

            self.root.after(1200, lambda: self.finish_upload(popup, key))

        except Exception as e:
            ui(f"Error: {e}", 0)

    


    def finish_upload(self, popup, key):
        popup.grab_release()
        popup.destroy()
        self.remove_card(key)

    def remove_card(self, key):
        card, _ = self.cards[key]
        card.destroy()
        del self.cards[key]
        del self.data[key]


# ---------------- RUN ----------------
if __name__ == "__main__":
    root = tk.Tk()
    app = ParcelUploaderApp(root)
    root.mainloop()
