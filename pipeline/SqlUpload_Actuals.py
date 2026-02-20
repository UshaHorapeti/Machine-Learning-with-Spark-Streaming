# SqlUpload_Actuals.py

import os
import sys
import time
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, r"C:\DP Waterfall Automation")
sys.path.insert(0, r"C:\DP Waterfall Automation\packages")
from mySQLClass import mySQLDB

# ---------- CONFIG ----------
SQL_UID = os.getenv("SQL_UID", "ISCBI001")
SQL_PWD = os.getenv("SQL_PWD", "$Refresh@01234567!")
AUTH_TYPE = "DB"

TABLE_NAME = "dbo.Demand_Actuals"

EXTRACTED_DIR = Path(r"F:\DP Waterfall\Transformed")
CHUNK_SIZE = 5000

EXPECTED_COLS = [
    "Source", "Snapshot", "Material", "Sales Organization",
    "Country", "Attribute", "Value", "BU"
]

ACTUAL_FILES = [
    "ACT_DemandBlank_Transformed.csv",
    "ACT_DemandNonBlank1_Transformed.csv",
    "ACT_DemandNonBlank2_Transformed.csv",
    "ACT_Unknown_Transformed.csv",
    "BPC_bySKU_Transformed.csv",
    "BPC_VAD_Transformed.csv",
]

# ---------- UTILITY ----------
def banner(msg: str):
    print(f"\n==== {msg} ====", flush=True)

def read_csvs():
    frames = []
    for fname in ACTUAL_FILES:
        path = EXTRACTED_DIR / fname
        if not path.exists():
            print(f"[WARN] Missing file: {fname}", flush=True)
            continue
        try:
            df = pd.read_csv(path, dtype=object, low_memory=False)
        except Exception as e:
            print(f"[ERROR] Could not read {fname}: {e}", flush=True)
            continue

        missing = [c for c in EXPECTED_COLS if c not in df.columns]
        if missing:
            print(f"[SKIP] {fname} missing columns {missing}", flush=True)
            continue

        df = df[EXPECTED_COLS].copy()
        frames.append(df)
        print(f"[OK] {fname} rows={len(df)}", flush=True)

    if not frames:
        return pd.DataFrame(columns=EXPECTED_COLS)

    return pd.concat(frames, ignore_index=True)

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    for c in ["Source", "Material", "Sales Organization", "Country", "BU"]:
        out[c] = out[c].astype(str).str.strip()
        out[c] = out[c].replace({"": None, "nan": None, "None": None})

    out["Snapshot"] = pd.to_datetime(out["Snapshot"], errors="coerce").dt.date
    out["Attribute"] = pd.to_datetime(out["Attribute"], errors="coerce").dt.date

    cleaned = (
        out["Value"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
    )
    out["Value"] = pd.to_numeric(cleaned, errors="coerce")

    return out.where(pd.notnull(out), None)

def upload(df: pd.DataFrame):
    if df.empty:
        print("[INFO] Nothing to upload.", flush=True)
        return

    db = mySQLDB(uid=SQL_UID, pwd=SQL_PWD, uid_type=AUTH_TYPE)
    schema, table = TABLE_NAME.split(".", 1)

    print(f"[INFO] Uploading {len(df)} rows to {TABLE_NAME}", flush=True)

    db.writeToDB(
        df,
        table_name=table,
        sql="",
        schema=schema,
        behaviour="append",
        chunks=CHUNK_SIZE
    )

    print("[DONE] Actuals upload complete.", flush=True)

# ---------- MAIN ----------
def main():
    t0 = time.time()

    banner("READ ACTUALS")
    df = read_csvs()
    print(f"[SUMMARY] Rows (raw): {len(df)}", flush=True)

    banner("CLEAN TYPES")
    df = coerce_types(df)

    banner("UPLOAD")
    upload(df)

    banner("DONE")
    print(f"[TIME] {time.time() - t0:.2f} sec", flush=True)

if __name__ == "__main__":
    main()