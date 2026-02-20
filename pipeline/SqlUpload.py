import os
import sys
import time
from pathlib import Path
import pandas as pd

sys.path.insert(0, r"C:\DP Waterfall Automation")
sys.path.insert(0, r"C:\DP Waterfall Automation\packages")
from mySQLClass import mySQLDB


# ---------- CONFIG ----------
SQL_UID = os.getenv("SQL_UID", "ISCBI001")
SQL_PWD = os.getenv("SQL_PWD", "$Refresh@01234567!")
AUTH_TYPE = "DB" 

# Targets
TABLE_ACTUALS  = "dbo.Demand_Actuals"
TABLE_FORECAST = "dbo.Demand_Forecast"

# Output Files location
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
FORECAST_FILES = [
    "FCST_DemandBlank_Transformed.csv",
    "FCST_DemandNonBlank1_Transformed.csv",
    "FCST_DemandNonBlank2_Transformed.csv",
    "FCST_DemandNonBlank3_Transformed.csv",
    "FCST_DemandNonBlank4_Transformed.csv",
    "FCST_DemandNonBlank5_Transformed.csv",
    "FCST_DemandNonBlank6_Transformed.csv",
    "FCST_DemandNonBlank7_Transformed.csv",
    "FCST_Unknown_Transformed.csv",
    "SAP_GERS_Transformed.csv",
]


# ---------- UTILITY ----------
def banner(msg: str):
    print(f"\n==== {msg} ====", flush=True)

def _read_csvs(file_list):
    """
    Read CSVs as-is (no cleaning, no type coercion). Only:
      - Warn if file missing
      - Require EXPECTED_COLS to exist
      - Select EXPECTED_COLS to match table order
    """
    frames = []
    for fname in file_list:
        path = EXTRACTED_DIR / fname
        if not path.exists():
            print(f"[WARN] Missing file: {fname}", flush=True)
            continue
        try:
            df = pd.read_csv(path, dtype=object, low_memory=False, na_filter=False)
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

def _upload(db: "mySQLDB", df: pd.DataFrame, table_full: str):
    """Append df into target table using mySQLClass."""
    if df.empty:
        print(f"[INFO] Nothing to upload for {table_full}.", flush=True)
        return
    schema, table = table_full.split(".", 1)
    print(f"[INFO] Writing {len(df)} rows to {table_full} in chunks of {CHUNK_SIZE} ...", flush=True)
    db.writeToDB(
        df,
        table_name=table,          
        sql="",
        schema=schema,
        behaviour="append",
        chunks=CHUNK_SIZE
    )
    print(f"[DONE] Append to {table_full} complete.", flush=True)

def _preview(db: "mySQLDB", table_full: str, top_n: int = 10):
    try:
        q = f"""
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
        SELECT TOP ({top_n})
               Source, Snapshot, Material, [Sales Organization], Country, Attribute, Value, BU
        FROM {table_full}
        ORDER BY
            CASE WHEN TRY_CONVERT(date, Snapshot) IS NOT NULL THEN 0 ELSE 1 END,
            TRY_CONVERT(date, Snapshot) DESC,
            Source;
        """
        preview = db.readfromDB(q)
        print(f"\n[PREVIEW] {table_full} (top {top_n}):", flush=True)
        print(preview.to_string(index=False), flush=True)
    except Exception as e:
        print(f"[WARN] Could not preview {table_full}: {e}", flush=True)


# ---------- MAIN ----------
def main():
    t0 = time.time()

    # Connect
    banner("CONNECT")
    db = mySQLDB(uid=SQL_UID, pwd=SQL_PWD, uid_type=AUTH_TYPE)
    print(f"[INFO] Passthrough mode: no transformations will be applied.", flush=True)

    # Read Actuals
    banner("READ ACTUALS")
    df_act = _read_csvs(ACTUAL_FILES)
    print(f"[SUMMARY] Actuals rows (raw): {len(df_act)}", flush=True)

    # Read Forecast
    banner("READ FORECAST")
    df_fcst = _read_csvs(FORECAST_FILES)
    print(f"[SUMMARY] Forecast rows (raw): {len(df_fcst)}", flush=True)

    # Upload Actuals
    banner("UPLOAD ACTUALS")
    _upload(db, df_act, TABLE_ACTUALS)
    _preview(db, TABLE_ACTUALS, top_n=10)

    # Upload Forecast
    banner("UPLOAD FORECAST")
    _upload(db, df_fcst, TABLE_FORECAST)
    _preview(db, TABLE_FORECAST, top_n=10)

    banner("DONE")
    print(f"[TIME] Total runtime: {time.time() - t0:.2f} sec", flush=True)


if __name__ == "__main__":
    main()