import sys
import time
from pathlib import Path
import pandas as pd

sys.path.insert(0, r"C:\DP Waterfall Automation")
sys.path.insert(0, r"C:\DP Waterfall Automation\packages")
from mySQLClass import mySQLDB

# ---------- CONSTANTS ----------
SQL_UID = "ISCBI001"
SQL_PWD = "$Refresh@01234567!"
AUTH_TYPE = "DB"

STAGING_SCHEMA = "dbo"
STAGING_TABLE  = "Staging"      
CHUNK_SIZE     = 5000           #chunk size
ROW_CAP        = 50_000         # hard cap for smoke test

EXTRACTED_DIR = Path(r"F:\DP Waterfall\Transformed")

EXPECTED_COLS = [
    "Source","Snapshot","Material","Sales Organization",
    "Country","Attribute","Value","BU"
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

def _read_csvs(file_list):
    frames = []
    for fname in file_list:
        path = EXTRACTED_DIR / fname
        if not path.exists():
            print(f"[WARN] Missing file: {fname}")
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as e:
            print(f"[ERROR] Could not read {fname}: {e}")
            continue
        missing = [c for c in EXPECTED_COLS if c not in df.columns]
        if missing:
            print(f"[SKIP] {fname} missing columns {missing}")
            continue
        df = df[EXPECTED_COLS].copy()
        df["Country"] = df["Country"].astype(str).str.strip()
        df["BU"]      = df["BU"].astype(str).str.strip()
        df["Value"]   = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)
        frames.append(df)
        print(f"[OK] {fname} rows={len(df)}")
    if not frames:
        return pd.DataFrame(columns=EXPECTED_COLS)
    return pd.concat(frames, ignore_index=True)

def _latest_snapshot(engine, table):
    sql1 = f"""
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SELECT CONVERT(varchar(10), MAX(TRY_CONVERT(date, Snapshot)), 101) AS SnapshotText
    FROM {table};
    """
    df1 = pd.read_sql(sql1, engine)
    s = (str(df1.iloc[0, 0]).strip() if not df1.empty and pd.notna(df1.iloc[0, 0]) else "")
    if not s:
        sql2 = f"""
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
        SELECT TOP (1) Snapshot
        FROM {table} WITH (NOLOCK)
        WHERE ISDATE(Snapshot) = 1
        ORDER BY TRY_CONVERT(date, Snapshot) DESC;
        """
        df2 = pd.read_sql(sql2, engine)
        s = (str(df2.iloc[0, 0]).strip() if not df2.empty and pd.notna(df2.iloc[0, 0]) else "")
        if not s:
            raise RuntimeError(f"[{table}] Snapshot detection failed: no parsable values.")
    return s

def _filter_exact_snapshot(df, snapshot_text):
    if df.empty:
        return df
    df = df.copy()
    snap_series = pd.to_datetime(df["Snapshot"], errors="coerce")
    target = pd.to_datetime(snapshot_text, errors="coerce").date()
    df["_SnapDate_"] = snap_series.dt.date
    return df[df["_SnapDate_"] == target].drop(columns=["_SnapDate_"], errors="ignore")

def _to_staging_types_keep_strings(df):
    """
    Align for dbo.Staging DDL (Value & BU are nchar(255)):
      - Snapshot, Attribute: date
      - Value, BU: strings
      - Other text cols: strings
    """
    out = df.copy()

    for col in ["Snapshot", "Attribute"]:
        dt = pd.to_datetime(out[col], errors="coerce", format="%m/%d/%Y")
        if dt.isna().mean() > 0.5:
            dt = pd.to_datetime(out[col], errors="coerce")
        out[col] = dt.dt.date.where(~dt.isna(), None)

    def _val_to_str(x):
        try:
            f = float(x)
            s = ("{:.6f}".format(f)).rstrip("0").rstrip(".")
            return s if s != "" else "0"
        except Exception:
            return str(x) if x is not None else ""
    out["Value"] = out["Value"].map(_val_to_str)

    for col in ["Source","Material","Sales Organization","Country","BU"]:
        out[col] = out[col].astype(str)

    return out

# ---------- Main ----------
def main():
    print("=== CONNECT via mySQLClass (hard-coded) ===")
    db = mySQLDB(uid=SQL_UID, pwd=SQL_PWD, uid_type=AUTH_TYPE)
    engine = db.engine

    # Latest snapshots
    snap_a = _latest_snapshot(engine, "dbo.Demand_Actuals")
    snap_f = _latest_snapshot(engine, "dbo.Demand_Forecast")
    print(f"Latest snapshots -> Actuals: {snap_a} | Forecast: {snap_f}")

    # Read files
    print("\n=== READ Actuals files ===")
    df_a_all = _read_csvs(ACTUAL_FILES)
    print("\n=== READ Forecast files ===")
    df_f_all = _read_csvs(FORECAST_FILES)

    # Filter to latest snapshot only
    print("\n[INFO] Filtering rows to exact latest snapshot dates.")
    df_a = _filter_exact_snapshot(df_a_all, snap_a)
    df_f = _filter_exact_snapshot(df_f_all, snap_f)

    df_all = pd.concat([df_a, df_f], ignore_index=True) if (not df_a.empty or not df_f.empty) else pd.DataFrame(columns=EXPECTED_COLS)
    print(f"[SUMMARY] Actuals rows={len(df_a)} | Forecast rows={len(df_f)} | TOTAL(after filter)={len(df_all)}")

    if df_all.empty:
        print("[DONE] Nothing to load (after filtering).")
        return

    # Enforce smoke-test cap
    if len(df_all) > ROW_CAP:
        print(f"[INFO] Smoke-test cap active: limiting rows to {ROW_CAP}.")
        df_all = df_all.iloc[:ROW_CAP].copy()

    # Prepare for dbo.Staging DDL
    df_stage = _to_staging_types_keep_strings(df_all)

    # Append only (NO TRUNCATE)
    print(f"[INFO] Writing {len(df_stage)} rows to {STAGING_SCHEMA}.{STAGING_TABLE} in chunks of {CHUNK_SIZE} ...")
    try:
        db.writeToDB(
            df_stage,
            table_name=STAGING_TABLE,
            sql="",                    # direct write
            schema=STAGING_SCHEMA,
            behaviour="append",
            chunks=CHUNK_SIZE
        )
        print("[DONE] Smoke-test insert completed.")
    except Exception as e:
        msg = str(e)
        if "40544" in msg or "size quota" in msg.lower():
            print("[STOP] Database size quota reached during insert (40544). Smoke-test aborted.")
        else:
            print(f"[ERROR] Smoke-test load failed: {e}")
        return

    # Preview a few rows from the staging table
    try:
        preview = db.readfromDB(
            f"SELECT TOP (25) * FROM {STAGING_SCHEMA}.{STAGING_TABLE} ORDER BY [Snapshot] DESC;"
        )
        print("\n[PREVIEW] First 25 rows from dbo.Staging:")
        print(preview.to_string(index=False))
    except Exception as e:
        print(f"[WARN] Could not preview staging: {e}")

if __name__ == "__main__":
    t0 = time.time()
    main()
    print(f"\n[TIME] Total runtime: {time.time() - t0:.2f} sec")
