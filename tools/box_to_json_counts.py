#!/usr/bin/env python3
import io
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
from boxsdk import Client, JWTAuth


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _parse_box_config() -> Dict[str, Any]:
    """
    BOX_CONFIG_JSON can be either:
    - the full JSON string of the Box config
    - OR a path to a JSON file (less common for GH Actions)
    """
    raw = _require_env("BOX_CONFIG_JSON").strip()

    # If it looks like a file path and exists, load from disk
    if (raw.endswith(".json") or raw.startswith("/")) and os.path.exists(raw):
        with open(raw, "r", encoding="utf-8") as f:
            return json.load(f)

    # Otherwise treat as JSON string
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            "BOX_CONFIG_JSON is not valid JSON. "
            "Make sure the GitHub secret contains the full JSON config string."
        ) from e


def _safe_to_int(x: Any) -> Optional[int]:
    try:
        if pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


def main() -> None:
    box_file_id = _require_env("BOX_FILE_ID").strip()
    box_user_id = _require_env("BOX_USER_ID").strip()
    lo_name = os.getenv("LO_NAME", "").strip()

    # Column names can be overridden if your sheet uses different headers
    col_lo = os.getenv("COL_LO", "Loan Officer")
    col_status = os.getenv("COL_STATUS", "Status")
    col_closing = os.getenv("COL_CLOSING", "Closing Date")

    config = _parse_box_config()

    # --- Box auth (impersonate your admin user) ---
    auth = JWTAuth.from_settings_dictionary(config)
    auth.authenticate_user(box_user_id)
    client = Client(auth)

    # Helpful debug
    me = client.user().get()
    print(f"AUTHENTICATED AS: id={me.id} name={me.name} login={me.login}")

    # --- Download XLSX into memory ---
    buf = io.BytesIO()
    # download_to expects a file-like object opened in binary write mode
    client.file(box_file_id).download_to(buf)
    buf.seek(0)

    # --- Read Excel ---
    # If you have multiple sheets, you can set SHEET_NAME env var
    sheet_name = os.getenv("SHEET_NAME", None)
    df = pd.read_excel(buf, sheet_name=sheet_name)

    # Normalize columns (strip spaces)
    df.columns = [str(c).strip() for c in df.columns]

    # Basic checks
    for needed in [col_status]:
        if needed not in df.columns:
            raise RuntimeError(
                f"Expected column '{needed}' not found. "
                f"Columns present: {list(df.columns)}"
            )

    # Optional filtering by LO
    if lo_name and col_lo in df.columns:
        df_lo = df[df[col_lo].astype(str).str.strip().str.lower() == lo_name.lower()].copy()
    else:
        df_lo = df.copy()

    # Try parsing closing date (if present)
    if col_closing in df_lo.columns:
        df_lo[col_closing] = pd.to_datetime(df_lo[col_closing], errors="coerce")

    now = datetime.now()
    current_year = now.year

    # Example KPI outputs (adjust to your needs)
    counts_by_status = (
        df_lo[col_status]
        .astype(str)
        .str.strip()
        .replace({"nan": ""})
        .value_counts(dropna=False)
        .to_dict()
    )

    closed_count_this_year: Optional[int] = None
    if col_closing in df_lo.columns:
        closed_mask = df_lo[col_status].astype(str).str.strip().str.lower().eq("closed")
        year_mask = df_lo[col_closing].dt.year.eq(current_year)
        closed_count_this_year = int((closed_mask & year_mask).sum())

    # You mentioned you want:
    # - count for Closed (current year)
    # - show all rows for "Clearing Conditions"
    # - show count or list for "Awaiting CTC" etc.
    clearing_rows = []
    awaiting_ctc_rows = []

    if col_status in df_lo.columns:
        # Collect full rows (as dicts) for specific statuses
        clearing_mask = df_lo[col_status].astype(str).str.strip().str.lower().eq("clearing conditions")
        awaiting_ctc_mask = df_lo[col_status].astype(str).str.strip().str.lower().eq("awaiting ctc")

        clearing_rows = df_lo[clearing_mask].fillna("").to_dict(orient="records")
        awaiting_ctc_rows = df_lo[awaiting_ctc_mask].fillna("").to_dict(orient="records")

    output: Dict[str, Any] = {
        "meta": {
            "generated_at": now.isoformat(timespec="seconds"),
            "box_file_id": box_file_id,
            "box_user_id": box_user_id,
            "loan_officer_filter": lo_name or None,
            "sheet_name": sheet_name,
        },
        "kpis": {
            "closed_count_current_year": closed_count_this_year,
            "counts_by_status": counts_by_status,
            "clearing_conditions_count": len(clearing_rows),
            "awaiting_ctc_count": len(awaiting_ctc_rows),
        },
        "tables": {
            "clearing_conditions": clearing_rows,
            "awaiting_ctc": awaiting_ctc_rows,
        },
    }

    # Write to repo root data.json
    out_path = os.path.join(os.getcwd(), "data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {out_path} (rows: {len(df_lo)})")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        raise
