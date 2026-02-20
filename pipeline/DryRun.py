
#!/usr/bin/env python
# Upload Demand Actuals & Forecast CSVs into SQL
# - Actuals CSVs  -> dbo.Demand_Actuals
# - Forecast CSVs -> dbo.Demand_Forecast
# - Force Snapshot to '02/01/2026' for ALL rows
# - If SQL column is DATE/DATETIME, bind a real date (2026-02-01)
#   else store the literal text '02/01/2026'
# - Append-only writes, chunked
# - Uses mySQLClass (SQLAlchemy + fast_executemany)

import os
import sys
import time
from datetime import date as _date
from pathlib import Path
import pandas as pd
import numpy as np

# Ensure Python can see your internal package (mySQLClass)
sys.path.insert(0, r"C:\DP Waterfall Automation")
sys.path.insert(0, r"C:\DP Waterfall Automation\packages")
from mySQLClass import mySQLDB


# ---------- CONFIG ----------
SQL_UID = os.getenv("SQL_UID", "ISCBI001")
SQL_PWD = os.getenv("SQL_PWD", "$Refresh@01234567!")
AUTH_TYPE = "DB"  

TABLE_ACTUALS  = "dbo.Demand_Actuals"
TABLE_FORECAST = "dbo.Demand_Forecast"

EXTRACTED_DIR = Path(r"F:\DP Waterfall\Transformed")

CHUNK_SIZE = 5000


SNAPSHOT_TEXT  = "03/01/2026"           
SNAPSHOT_PYDATE = _date(2026, 3, 1)     

# Expected schema in CSVs (order will be respected)
EXPECTED_COLS = [
    "Source", "Snapshot", "Material", "Sales Organization",
    "Country", "Attribute", "Value", "BU"
]

# File lists (matching your QC tool classification)
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


# ---------- UTIL ----------
def banner(msg: str):
    print(f"\n==== {msg} ====", flush=True)

def _clean_text(x):
    """Trim strings; keep None/NaN as NULL; avoid literal 'nan'."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s

def _read_csvs(file_list):
    frames = []
    for fname in file_list:
        path = EXTRACTED_DIR / fname
        if not path.exists():
            print(f"[WARN] Missing file: {fname}", flush=True)
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as e:
            print(f"[ERROR] Could not read {fname}: {e}", flush=True)
            continue

        missing = [c for c in EXPECTED_COLS if c not in df.columns]
        if missing:
            print(f"[SKIP] {fname} missing columns {missing}", flush=True)
            continue

        # Select and basic normalize
        df = df[EXPECTED_COLS].copy()
        df["Country"] = df["Country"].astype(str).str.strip()
        df["BU"]      = df["BU"].astype(str).str.strip()
        df["Value"]   = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)

        frames.append(df)
        print(f"[OK] {fname} rows={len(df)}", flush=True)

    if not frames:
        return pd.DataFrame(columns=EXPECTED_COLS)
    return pd.concat(frames, ignore_index=True)

def _get_sql_types(db: mySQLDB, table_full: str) -> dict:
    """
    Returns {column_lower: datatype_lower} for the SQL table.
    """
    schema, table = table_full.split(".", 1)
    q = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}';
    """
    df = db.readfromDB(q)
    types = {str(r["COLUMN_NAME"]).strip().lower(): str(r["DATA_TYPE"]).strip().lower()
             for _, r in df.iterrows()}
    return types

def _is_date_type(sql_type: str) -> bool:
    return sql_type in {"date", "datetime", "smalldatetime", "datetime2"}

def _is_numeric_type(sql_type: str) -> bool:
    return sql_type in {"int", "bigint", "smallint", "tinyint", "float", "real", "numeric", "decimal", "money", "smallmoney"}

def _coerce_df_to_sql_types(df: pd.DataFrame, sql_types: dict) -> pd.DataFrame:
    """
    Coerce dataframe columns to align with SQL table types:
      - Snapshot: date object if SQL is date-like, else literal '02/01/2026'
      - Attribute: parse as date if SQL is date-like; else clean text
      - Sales Organization: numeric if SQL numeric; else clean text
      - Value: numeric if SQL numeric; else text
      - Others: cleaned text
    """
    out = df.copy()

    # Normalize
    def t(col_name):
        return sql_types.get(str(col_name).lower())

    # Snapshot
    snap_type = t("Snapshot")
    if _is_date_type(snap_type or ""):
        out["Snapshot"] = SNAPSHOT_PYDATE
    else:
        out["Snapshot"] = SNAPSHOT_TEXT

    # Attribute
    attr_type = t("Attribute")
    if _is_date_type(attr_type or ""):
        attr = pd.to_datetime(out["Attribute"], errors="coerce", format="%m/%d/%Y")
        attr = attr.fillna(pd.to_datetime(out["Attribute"], errors="coerce"))
        out["Attribute"] = attr.dt.date.where(~attr.isna(), None)
    else:
        out["Attribute"] = out["Attribute"].map(_clean_text)

    # Sales Organization
    so_type = t("Sales Organization")
    if _is_numeric_type(so_type or ""):
        so_num = pd.to_numeric(out["Sales Organization"], errors="coerce")
        out["Sales Organization"] = so_num.round(0).astype("Int64")
    else:
        out["Sales Organization"] = out["Sales Organization"].map(_clean_text)

    # Value
    val_type = t("Value")
    if _is_numeric_type(val_type or ""):
        out["Value"] = pd.to_numeric(out["Value"], errors="coerce")
    else:
        out["Value"] = out["Value"].apply(lambda v: None if pd.isna(v) else str(v).strip())

    # Other text columns
    for col in ["Source", "Material", "Country", "BU"]:
        if col in out.columns:
            out[col] = out[col].map(_clean_text)

    out = out[[c for c in EXPECTED_COLS if c in out.columns]]
    return out

def _upload(db: mySQLDB, df: pd.DataFrame, table_full: str):
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

def _preview(db: mySQLDB, table_full: str, top_n: int = 10):
    try:
        q = f"""
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
        SELECT TOP ({top_n})
               Source, Snapshot, Material, [Sales Organization], Country, Attribute, Value, BU
        FROM {table_full}
        ORDER BY TRY_CONVERT(date, Snapshot) DESC, Source;
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
    print(f"[INFO] Forcing Snapshot -> {SNAPSHOT_TEXT}", flush=True)

    # Read Actuals
    banner("READ ACTUALS")
    df_act = _read_csvs(ACTUAL_FILES)
    print(f"[SUMMARY] Actuals rows (raw): {len(df_act)}", flush=True)

    # Read Forecast
    banner("READ FORECAST")
    df_fcst = _read_csvs(FORECAST_FILES)
    print(f"[SUMMARY] Forecast rows (raw): {len(df_fcst)}", flush=True)

    banner("FETCH SQL TYPES")
    sql_types_act  = _get_sql_types(db, TABLE_ACTUALS)
    sql_types_fcst = _get_sql_types(db, TABLE_FORECAST)

    banner("ALIGN TO SQL SCHEMA")
    df_act_coerced  = _coerce_df_to_sql_types(df_act,  sql_types_act)
    df_fcst_coerced = _coerce_df_to_sql_types(df_fcst, sql_types_fcst)

    # Upload Actuals
    banner("UPLOAD ACTUALS")
    _upload(db, df_act_coerced, TABLE_ACTUALS)
    _preview(db, TABLE_ACTUALS, top_n=10)

    # Upload Forecast
    banner("UPLOAD FORECAST")
    _upload(db, df_fcst_coerced, TABLE_FORECAST)
    _preview(db, TABLE_FORECAST, top_n=10)

    banner("DONE")
    print(f"[TIME] Total runtime: {time.time() - t0:.2f} sec", flush=True)


if __name__ == "__main__":
    main()
