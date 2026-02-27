import json
import os
from datetime import datetime

import pandas as pd
from boxsdk import JWTAuth, Client

BOX_CONFIG_JSON = os.environ["BOX_CONFIG_JSON"]
BOX_FILE_ID = os.environ["BOX_FILE_ID"]

# TODO: change these if your sheet headers differ
COL_LO = os.environ.get("COL_LO", "Loan Officer")
COL_STATUS = os.environ.get("COL_STATUS", "Status")
COL_CLOSING = os.environ.get("COL_CLOSING", "Closing Date")

LO_NAME = os.environ.get("LO_NAME", "Anfal Kothawala")
FOCUS_STATUSES = ["Awaiting CTC", "Clearing Conditions"]

def main():
    auth = JWTAuth.from_settings_dictionary(json.loads(BOX_CONFIG_JSON))
    client = Client(auth)
    auth.authenticate_instance()

    # Download XLSX from Box
    xlsx_path = "source.xlsx"
    with open(xlsx_path, "wb") as f:
        client.file(BOX_FILE_ID).download_to(f)

    df = pd.read_excel(xlsx_path)

    # Normalize
    if COL_LO in df.columns:
        df[COL_LO] = df[COL_LO].astype(str).str.strip()
    if COL_STATUS in df.columns:
        df[COL_STATUS] = df[COL_STATUS].astype(str).str.strip()

    # Filter to your loans
    my = df[df[COL_LO].astype(str).str.contains(LO_NAME, na=False)] if COL_LO in df.columns else df

    # Counts
    closed_ytd = int((my[COL_STATUS].astype(str).str.lower() == "closed").sum()) if COL_STATUS in my.columns else 0

    status_counts = {}
    if COL_STATUS in my.columns:
        for s in FOCUS_STATUSES:
            status_counts[s] = int((my[COL_STATUS] == s).sum())

    payload = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "loan_officer": LO_NAME,
        "closed_ytd": closed_ytd,
        "status_counts": status_counts,
        "rows": []  # counts-only for public safety
    }

    with open("data.json", "w") as f:
        json.dump(payload, f, indent=2)

if __name__ == "__main__":
    main()
