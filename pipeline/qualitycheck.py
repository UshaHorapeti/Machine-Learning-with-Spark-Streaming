
#!/usr/bin/env python
import sys
from pathlib import Path
import time
import numpy as np
import pandas as pd

# mySQLClass path
sys.path.insert(0, r"C:\DP Waterfall Automation")
sys.path.insert(0, r"C:\DP Waterfall Automation\packages")
from mySQLClass import mySQLDB

SQL_UID = "ISCBI001"
SQL_PWD = "$Refresh@01234567!"
AUTH_TYPE = "DB"

TABLE_ACTUALS = "dbo.Demand_Actuals"
TABLE_FORECAST = "dbo.Demand_Forecast"

EXTRACTED_DIR = Path(r"F:\DP Waterfall\Transformed")
OUTPUT_DIR = Path(r"F:\DP Waterfall\Validation")
QC_THRESHOLD = 0.15

EXPECTED_COLS = ["Source","Snapshot","Material","Sales Organization","Country","Attribute","Value","BU"]

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

def fmt_num(n: float) -> str:
    if n is None or pd.isna(n):
        return ""
    try:
        n = float(n)
    except Exception:
        return str(n)
    sign = "-" if n < 0 else ""
    a = abs(n)
    if a >= 1_000_000_000:
        return f"{sign}{a/1_000_000_000:.2f} B"
    if a >= 1_000_000:
        return f"{sign}{a/1_000_000:.2f} M"
    if a >= 1_000:
        return f"{sign}{a/1_000:.2f} K"
    return f"{n:,.0f}" if a >= 1 else f"{n:.6f}".rstrip("0").rstrip(".")

def fmt_pct(p: float) -> str:
    try:
        if p is None or pd.isna(p):
            return ""
        if np.isinf(p):
            return "∞"
        return f"{p*100:.2f}%"
    except Exception:
        return str(p)

def banner(msg: str):
    print(f"\n==== {msg} ====", flush=True)

def _latest_snapshot(engine, table: str) -> str:
    sql1 = f"""
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SELECT CONVERT(varchar(10), MAX(TRY_CONVERT(date, Snapshot)), 101) AS SnapshotText
    FROM {table};
    """
    df1 = pd.read_sql(sql1, engine)
    s = (str(df1.iloc[0,0]).strip() if not df1.empty and pd.notna(df1.iloc[0,0]) else "")
    if not s:
        sql2 = f"""
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
        SELECT TOP (1) Snapshot
        FROM {table} WITH (NOLOCK)
        WHERE ISDATE(Snapshot) = 1
        ORDER BY TRY_CONVERT(date, Snapshot) DESC;
        """
        df2 = pd.read_sql(sql2, engine)
        s = (str(df2.iloc[0,0]).strip() if not df2.empty and pd.notna(df2.iloc[0,0]) else "")
    if not s:
        raise RuntimeError(f"[{table}] Snapshot detection failed: no parsable values.")
    return s

def _read_latest_df(engine, table: str, snapshot_text: str) -> pd.DataFrame:
    snap_q = snapshot_text.replace("'", "''")
    sql = f"""
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SELECT Source, Snapshot, Material, [Sales Organization], Country, Attribute, Value, BU
    FROM {table} WITH (NOLOCK)
    WHERE TRY_CONVERT(date, Snapshot) = TRY_CONVERT(date, '{snap_q}');
    """
    df = pd.read_sql(sql, engine)
    df.columns = [str(c).strip() for c in df.columns]
    df["Country"] = df["Country"].astype(str).str.strip()
    df["BU"] = df["BU"].astype(str).str.strip()
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)
    return df

def get_latest_sql_frames() -> dict:
    banner("SQL CONNECT")
    db = mySQLDB(uid=SQL_UID, pwd=SQL_PWD, uid_type=AUTH_TYPE)
    snap_a = _latest_snapshot(db.engine, TABLE_ACTUALS)
    df_a = _read_latest_df(db.engine, TABLE_ACTUALS, snap_a)
    df_a.attrs["latest_snapshot"] = snap_a
    snap_f = _latest_snapshot(db.engine, TABLE_FORECAST)
    df_f = _read_latest_df(db.engine, TABLE_FORECAST, snap_f)
    df_f.attrs["latest_snapshot"] = snap_f
    return {"actuals": df_a, "forecast": df_f}

def read_csv_files(file_list) -> pd.DataFrame:
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
        df["Country"] = df["Country"].astype(str).str.strip()
        df["BU"] = df["BU"].astype(str).str.strip()
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)
        frames.append(df[EXPECTED_COLS].copy())
        print(f"[OK] {fname} rows={len(df)}", flush=True)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=EXPECTED_COLS)

def aggregate(df: pd.DataFrame) -> dict:
    if df.empty:
        empty = pd.DataFrame(columns=["Key","value_sum"])
        return {"BU": empty.copy(), "Country": empty.copy(), "BU+Country": empty.copy()}
    df = df.copy()
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)
    df["Country"] = df["Country"].fillna("").astype(str).str.strip()
    df["BU"] = df["BU"].fillna("").astype(str).str.strip()
    agg_bu = (df.groupby("BU", dropna=False, sort=False)["Value"].sum()
                .reset_index().rename(columns={"BU":"Key","Value":"value_sum"}))
    agg_country = (df.groupby("Country", dropna=False, sort=False)["Value"].sum()
                .reset_index().rename(columns={"Country":"Key","Value":"value_sum"}))
    df["BU_Country"] = df["BU"].str.strip() + " | " + df["Country"].str.strip()
    agg_bucountry = (df.groupby("BU_Country", dropna=False, sort=False)["Value"].sum()
                .reset_index().rename(columns={"BU_Country":"Key","Value":"value_sum"}))
    return {"BU": agg_bu, "Country": agg_country, "BU+Country": agg_bucountry}

def compare_aggregates(agg_extracted: dict, agg_sql: dict) -> pd.DataFrame:
    rows = []
    for dim in ["BU","Country","BU+Country"]:
        left = agg_extracted[dim].set_index("Key")
        right = agg_sql[dim].set_index("Key")
        keys = sorted(set(left.index) | set(right.index))
        for k in keys:
            ext_val = float(left.loc[k,"value_sum"]) if k in left.index else 0.0
            sql_val = float(right.loc[k,"value_sum"]) if k in right.index else 0.0
            if sql_val == 0.0:
                pct = 0.0 if ext_val == 0.0 else float("inf")
                qc = "PASS" if ext_val == 0.0 else "FAIL"
            else:
                pct = (ext_val - sql_val) / sql_val
                qc = "PASS" if abs(pct) <= QC_THRESHOLD else "FAIL"
            rows.append({
                "Dimension": dim,
                "Key": k,
                "Extracted value": ext_val,
                "Prev snapshot value": sql_val,
                "Pct diff": pct,
                "QC": qc,
            })
    comp = pd.DataFrame(rows)
    comp["Pct diff"] = pd.to_numeric(comp["Pct diff"], errors="coerce")
    abs_arr = comp["Pct diff"].astype("float64").values
    inf_mask = np.isinf(abs_arr)
    if inf_mask.any():
        abs_arr[inf_mask] = np.nan
    comp["abs_pct"] = np.abs(abs_arr)
    comp = comp.sort_values(["Dimension","abs_pct"], ascending=[True, False]).drop(columns=["abs_pct"]).reset_index(drop=True)
    return comp

def format_for_output(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Extracted value"] = out["Extracted value"].map(fmt_num)
    out["Prev snapshot value"] = out["Prev snapshot value"].map(fmt_num)
    out["Pct diff"] = out["Pct diff"].map(fmt_pct)
    return out

def main():
    banner("START QC")
    t0 = time.time()

    dfs_sql = get_latest_sql_frames()

    df_ext_a = read_csv_files(ACTUAL_FILES)
    df_ext_f = read_csv_files(FORECAST_FILES)

    comp_a = compare_aggregates(aggregate(df_ext_a), aggregate(dfs_sql["actuals"])).assign(Group="Actuals")
    comp_f = compare_aggregates(aggregate(df_ext_f), aggregate(dfs_sql["forecast"])).assign(Group="Forecast")

    preview_a = format_for_output(comp_a.head(25))
    preview_f = format_for_output(comp_f.head(25))

    print("\n[Actuals preview (first 25 rows)]:", flush=True)
    print(preview_a.to_string(index=False), flush=True)
    print("\n[Forecast preview (first 25 rows)]:", flush=True)
    print(preview_f.to_string(index=False), flush=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_a = OUTPUT_DIR / "Actuals Validation.csv"
    out_f = OUTPUT_DIR / "Forecast Validation.csv"

    comp_a_full = format_for_output(comp_a)
    comp_f_full = format_for_output(comp_f)
    comp_a_full.to_csv(out_a, index=False, encoding="utf-8-sig")
    comp_f_full.to_csv(out_f, index=False, encoding="utf-8-sig")

    print(f"\n[SAVED] {out_a}")
    print(f"[SAVED] {out_f}")
    print(f"[TIME] Total runtime: {time.time() - t0:.2f} sec", flush=True)
    banner("DONE")

if __name__ == "__main__":
    main()
