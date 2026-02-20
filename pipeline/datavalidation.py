
#!/usr/bin/env python
# datavalidation.py — Automated Data Validation for CSV extracts vs SQL latest snapshot
# Dimensions: BU + Fiscal Year (FY derived from Attribute; use max Attribute year)
# Compares automated process (CSV extracts) vs old process (SQL filtered to exact same snapshot date)
# Saves RAW and PRETTY CSVs for Actuals and Forecast with human-readable values and variances.

import os
import sys
import time
from pathlib import Path
import re
import calendar
from datetime import datetime
import numpy as np
import pandas as pd

# Ensure Python can see your internal package (mySQLClass) — SAME as your working scripts
sys.path.insert(0, r"C:\DP Waterfall Automation")
sys.path.insert(0, r"C:\DP Waterfall Automation\packages")

# ---------- Console helpers ----------
def banner(msg: str):
    print(f"\n==== {msg} ====", flush=True)

def t_start():
    return time.time()

def t_end(ts, label: str):
    print(f"[TIME] {label} took {time.time() - ts:.2f} sec", flush=True)

# ---------- Import DB wrapper (use ONLY mySQLClass) ----------
try:
    from mySQLClass import mySQLDB
except Exception as e:
    banner("IMPORT ERROR")
    print("Could not import mySQLClass. Check: C:\\DP Waterfall Automation\\packages\\mySQLClass.py", flush=True)
    print(f"Details: {e}", flush=True)
    sys.exit(2)

# ---------- Hardcoded Config ----------
SQL_UID = "ISCBI001"
SQL_PWD = "$Refresh@01234567!"
AUTH_TYPE = "DB"  # DB = SQL (Database) authentication

TABLE_ACTUALS = "dbo.Demand_Actuals"
TABLE_FORECAST = "dbo.Demand_Forecast"

EXTRACTED_DIR = Path(r"F:\DP Waterfall\Transformed")     
OUTPUT_DIR    = Path(r"F:\DP Waterfall\Validation")       

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

# ---------- Formatting (human-readable) ----------
def fmt_int(n) -> str:
    try:
        return f"{int(round(float(n))):,}"
    except Exception:
        return "-"

def fmt_val(n) -> str:
    """
    Human-readable EA Units: thousands separators, no scientific notation.
    Show decimals only if <1.
    """
    try:
        x = float(n)
        if abs(x) >= 1:
            return f"{x:,.0f}"
        else:
            return f"{x:.6f}".rstrip("0").rstrip(".")
    except Exception:
        return "-"

def fmt_pct(p) -> str:
    """
    Percent with 2 decimals. If baseline is 0 and numerator != 0, show '-'.
    If NaN/inf, show '-'.
    """
    if p is None:
        return "-"
    try:
        if isinstance(p, float) and (np.isnan(p) or np.isinf(p)):
            return "-"
        return f"{p*100:.2f}%"
    except Exception:
        return "-"

# ---------- Attribute → FY helpers ----------
FY_RE_2DIG = re.compile(r"\bFY\s*([0-9]{2})\b", re.IGNORECASE)
FY_RE_4DIG = re.compile(r"\bFY\s*([2][0][0-9]{2})\b", re.IGNORECASE)
YEAR4_RE   = re.compile(r"\b(20[0-9]{2})\b")

def _extract_endyear_from_attribute(attr: str):
    """
    Parse the Attribute text and return an 'end-year' as integer (e.g., 2026) if found, else None.
    Accepts patterns like: 'FY26', 'FY2026', 'Current month to FY26', '... 2026 ...'
    """
    if not isinstance(attr, str) or not attr.strip():
        return None

    s = attr.strip()

    # Prefer explicit FY patterns
    m4 = FY_RE_4DIG.search(s)
    if m4:
        y = int(m4.group(1))
        return y

    m2 = FY_RE_2DIG.search(s)
    if m2:
        y2 = int(m2.group(1))
        # Map 'FY26' -> 2026
        return 2000 + y2

    # Fallback: any 4-digit year between 2000-2099
    mY = YEAR4_RE.search(s)
    if mY:
        y = int(mY.group(1))
        return y

    return None

def derive_global_fy_label_from_attribute(df_csv: pd.DataFrame, df_sql: pd.DataFrame) -> str:
    """
    Derive FY label ('FYxx') from the maximum year found in Attribute.
    Prefer CSV (automated process); if none found there, fallback to SQL (old process).
    If nothing is found, return an empty string.
    """
    def max_year_from(df):
        if df is None or df.empty or "Attribute" not in df.columns:
            return None
        years = []
        for s in df["Attribute"]:
            y = _extract_endyear_from_attribute(s)
            if isinstance(y, int) and 2000 <= y <= 2099:
                years.append(y)
        return max(years) if years else None

    y_csv = max_year_from(df_csv)
    y_sql = max_year_from(df_sql)

    end_year = y_csv if y_csv is not None else y_sql
    if end_year is None:
        return ""
    return f"FY{end_year % 100:02d}"

# ---------- Build last-12-month Attribute filter (for Actuals SQL) ----------
def _last_12_month_patterns(snapshot_text: str) -> re.Pattern:
    """
    Generate a single regex that matches any common representation of months
    for the last 12 months relative to snapshot_text (inclusive).
    Patterns include:
      - YYYYMM, YYYY-MM, YYYY/MM
      - MM/YYYY
      - Mon YYYY (Jan 2025), Month YYYY (January 2025)
      - Mon-YY (Jan-25), Mon YY (Jan 25)
    """
    snap_dt = pd.to_datetime(snapshot_text, errors="coerce")
    if pd.isna(snap_dt):
        # If snapshot can't be parsed, don't filter
        return re.compile(r".*", re.IGNORECASE)

    # Build (year, month) pairs for last 12 months inclusive
    months = []
    y = snap_dt.year
    m = snap_dt.month
    for i in range(12):
        yr = y if m - i > 0 else y - 1
        mo = (m - i - 1) % 12 + 1
        if m - i <= 0:
            yr = y - 1
        months.append((yr, mo))

    tokens = []
    for yr, mo in months:
        yy = yr % 100
        mon_abbr = calendar.month_abbr[mo]   # Jan
        mon_name = calendar.month_name[mo]   # January

        # Numeric formats
        tokens.append(f"{yr}{mo:02d}")       # YYYYMM
        tokens.append(f"{yr}-{mo:02d}")      # YYYY-MM
        tokens.append(f"{yr}/{mo:02d}")      # YYYY/MM
        tokens.append(f"{mo:02d}/{yr}")      # MM/YYYY

        # Name formats
        tokens.append(f"{mon_abbr} {yr}")    # Jan 2025
        tokens.append(f"{mon_name} {yr}")    # January 2025
        tokens.append(f"{mon_abbr}-{yy:02d}")# Jan-25
        tokens.append(f"{mon_abbr} {yy:02d}")# Jan 25

    # Build one big OR-regex (escape tokens to be safe)
    alt = "|".join(re.escape(t) for t in set(tokens))
    return re.compile(alt, re.IGNORECASE)

def filter_actuals_attribute_last_12_months(df_sql_actuals: pd.DataFrame, snapshot_text: str) -> pd.DataFrame:
    """
    Keep only SQL Actuals rows whose Attribute contains any token for the last 12 months
    (relative to snapshot_text). If Attribute is NaN or no match, the row is dropped.
    If snapshot_text cannot be parsed, the original frame is returned (no filter).
    """
    if df_sql_actuals.empty or "Attribute" not in df_sql_actuals.columns:
        return df_sql_actuals
    pattern = _last_12_month_patterns(snapshot_text)
    mask = df_sql_actuals["Attribute"].astype(str).str.contains(pattern, regex=True, na=False)
    filtered = df_sql_actuals[mask].copy()
    if filtered.empty:
        print("[WARN] SQL Actuals: Attribute filter for last 12 months returned 0 rows.", flush=True)
    return filtered

# ---------- SQL Helpers (using mySQLClass engine ONLY) ----------
def _latest_snapshot(engine, table: str) -> str:
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
    engine = db.engine

    snap_a = _latest_snapshot(engine, TABLE_ACTUALS)
    df_a = _read_latest_df(engine, TABLE_ACTUALS, snap_a)
    df_a.attrs["latest_snapshot"] = snap_a

    snap_f = _latest_snapshot(engine, TABLE_FORECAST)
    df_f = _read_latest_df(engine, TABLE_FORECAST, snap_f)
    df_f.attrs["latest_snapshot"] = snap_f

    return {"actuals": df_a, "forecast": df_f}

# ---------- CSV Readers ----------
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

# ---------- Snapshot filter (date must match exactly) ----------
def filter_to_snapshot(df: pd.DataFrame, snapshot_text: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["__SnapDate__"] = pd.to_datetime(df["Snapshot"], errors="coerce").dt.date
    target_date = pd.to_datetime(snapshot_text, errors="coerce").date()
    filtered = df[df["__SnapDate__"] == target_date].drop(columns=["__SnapDate__"], errors="ignore")
    if filtered.empty:
        print(f"[WARN] No CSV rows for exact Snapshot date: {snapshot_text}", flush=True)
    return filtered

# ---------- Aggregation (BU only) ----------
def aggregate_bu(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns DataFrame with columns: BU, value_sum (EA Units), row_count
    Grouped by BU only.
    """
    if df.empty:
        return pd.DataFrame(columns=["BU", "value_sum", "row_count"])
    df = df.copy()
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)
    df["BU"] = df["BU"].fillna("").astype(str).str.strip()
    agg = (
        df.groupby(["BU"], dropna=False, sort=False)
          .agg(value_sum=("Value", "sum"), row_count=("Value", "size"))
          .reset_index()
    )
    return agg

# ---------- Build validation table with variances (BU + FY column) ----------
def build_validation_with_variance_bu(
    agg_csv: pd.DataFrame,
    agg_sql: pd.DataFrame,
    fy_label: str
) -> pd.DataFrame:
    """
    Returns a dataframe in the requested column order, with absolute and % variances.
    - agg_csv: automated process (extracted files) aggregated by BU
    - agg_sql: old process (SQL) aggregated by BU
    - fy_label: 'FYxx' label derived from max(Attribute year)
    """
    left = agg_csv.rename(columns={"value_sum": "EA_new", "row_count": "RC_new"})
    right = agg_sql.rename(columns={"value_sum": "EA_old", "row_count": "RC_old"})
    comp = pd.merge(left, right, on=["BU"], how="outer")

    # fill NaNs with zeros to compute variances
    for col in ["RC_new", "RC_old", "EA_new", "EA_old"]:
        comp[col] = pd.to_numeric(comp[col], errors="coerce").fillna(0)

    # Variances
    comp["Variance(Row count)"] = comp["RC_new"] - comp["RC_old"]
    # Row count % variance: baseline is old (SQL)
    comp["Variance(Row count) %"] = np.where(
        comp["RC_old"] == 0,
        np.nan,  # will format as '-'
        (comp["RC_new"] - comp["RC_old"]) / comp["RC_old"]
    )
    comp["Variance(EA Units)"] = comp["EA_new"] - comp["EA_old"]
    comp["Variance(EA Units) %"] = np.where(
        comp["EA_old"] == 0.0,
        np.nan,
        (comp["EA_new"] - comp["EA_old"]) / comp["EA_old"]
    )

    # Dynamic FY label in headers
    fy_hdr = fy_label if fy_label else "FY"
    new_units_hdr = f"EA Units(Current month to {fy_hdr}) as per new automated process"
    old_units_hdr = f"EA Units(Current month to {fy_hdr}) as per old process"

    # Insert FY beside BU (same FY for this run, based on max(Attribute year))
    comp.insert(1, "Fiscal Year", fy_label)

    # Final column order
    comp = comp[[
        "BU",
        "Fiscal Year",
        "RC_new",
        "RC_old",
        "Variance(Row count)",
        "Variance(Row count) %",
        "EA_new",
        "EA_old",
        "Variance(EA Units)",
        "Variance(EA Units) %",
    ]]

    comp = comp.rename(columns={
        "RC_new": "Row count as per new automated process",
        "RC_old": "Row count as per old process",
        "EA_new": new_units_hdr,
        "EA_old": old_units_hdr
    })

    # Sort for readability
    comp = comp.sort_values(["BU"]).reset_index(drop=True)
    return comp

# ---------- Create PRETTY (formatted) copy ----------
def make_pretty(df: pd.DataFrame) -> pd.DataFrame:
    pretty = df.copy()

    # Format counts and units with thousands separators (no scientific)
    count_cols = [
        "Row count as per new automated process",
        "Row count as per old process",
        "Variance(Row count)"
    ]
    unit_cols = [
        c for c in df.columns if "EA Units(" in c
    ] + ["Variance(EA Units)"]

    for c in count_cols:
        pretty[c] = pretty[c].map(fmt_int)

    for c in unit_cols:
        pretty[c] = pretty[c].map(fmt_val)

    # Percent columns
    pct_cols = ["Variance(Row count) %", "Variance(EA Units) %"]
    for c in pct_cols:
        pretty[c] = pretty[c].map(fmt_pct)

    return pretty

# ---------- Main ----------
def main():
    banner("DATA VALIDATION — START")
    t0 = t_start()

    # SQL side (mySQLClass only)
    t_sql = t_start()
    try:
        dfs_sql = get_latest_sql_frames()
    except Exception as e:
        banner("SQL CONNECT ERROR")
        print("Could not connect to SQL via mySQLClass.", flush=True)
        print(f"Details: {e}", flush=True)
        return
    t_end(t_sql, "SQL read")

    snap_a = dfs_sql['actuals'].attrs.get('latest_snapshot')
    snap_f = dfs_sql['forecast'].attrs.get('latest_snapshot')
    print(f"Snapshots -> Actuals: {snap_a} | Forecast: {snap_f}", flush=True)

    # NEW: Filter SQL Actuals by Attribute to last 12 months (relative to latest snapshot)
    df_sql_actuals_filtered = filter_actuals_attribute_last_12_months(dfs_sql["actuals"], snap_a)
    dfs_sql["actuals"] = df_sql_actuals_filtered  # replace with filtered frame

    # CSV side (read all, then filter to exact snapshots)
    t_ext = t_start()
    print("\nReading Actuals CSV files...", flush=True)
    df_ext_a_all = read_csv_files(ACTUAL_FILES)

    print("\nReading Forecast CSV files...", flush=True)
    df_ext_f_all = read_csv_files(FORECAST_FILES)
    t_end(t_ext, "CSV read")

    # strict snapshot alignment of CSV extracts (unchanged)
    df_ext_a = filter_to_snapshot(df_ext_a_all, snap_a)
    df_ext_f = filter_to_snapshot(df_ext_f_all, snap_f)

    print(f"Summary AFTER filter: Actuals rows={len(df_ext_a)} (date={snap_a}) | Forecast rows={len(df_ext_f)} (date={snap_f})", flush=True)
    print(f"SQL Actuals after Attribute(last-12-month) filter: rows={len(dfs_sql['actuals'])}", flush=True)

    # ----- Derive FY label from Attribute (max year) -----
    fy_a = derive_global_fy_label_from_attribute(df_ext_a, dfs_sql["actuals"])
    fy_f = derive_global_fy_label_from_attribute(df_ext_f, dfs_sql["forecast"])
    if not fy_a:
        print("[WARN] Could not derive FY from Attribute for Actuals; FY column/header will show 'FY'.", flush=True)
    if not fy_f:
        print("[WARN] Could not derive FY from Attribute for Forecast; FY column/header will show 'FY'.", flush=True)

    # Aggregate by BU only
    agg_csv_a = aggregate_bu(df_ext_a)
    agg_sql_a = aggregate_bu(dfs_sql["actuals"])  # SQL Actuals already filtered to last 12 months
    out_a_raw = build_validation_with_variance_bu(agg_csv_a, agg_sql_a, fy_label=fy_a)

    agg_csv_f = aggregate_bu(df_ext_f)
    agg_sql_f = aggregate_bu(dfs_sql["forecast"])  # Forecast unchanged
    out_f_raw = build_validation_with_variance_bu(agg_csv_f, agg_sql_f, fy_label=fy_f)

    # Pretty (human-readable) versions
    out_a_pretty = make_pretty(out_a_raw)
    out_f_pretty = make_pretty(out_f_raw)

    # Save outputs — in F:\DP Waterfall\Validation
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    p_a_raw = OUTPUT_DIR / "Data_Validation_Actuals_BU_FY_raw.csv"
    p_a_pre = OUTPUT_DIR / "Data_Validation_Actuals_BU_FY_pretty.csv"
    p_f_raw = OUTPUT_DIR / "Data_Validation_Forecast_BU_FY_raw.csv"
    p_f_pre = OUTPUT_DIR / "Data_Validation_Forecast_BU_FY_pretty.csv"

    out_a_raw.to_csv(p_a_raw, index=False)
    out_a_pretty.to_csv(p_a_pre, index=False)
    out_f_raw.to_csv(p_f_raw, index=False)
    out_f_pretty.to_csv(p_f_pre, index=False)

    banner("DATA VALIDATION — SUMMARY")
    print("Saved:", flush=True)
    print(f"  - {p_a_raw} (rows={len(out_a_raw)})", flush=True)
    print(f"  - {p_a_pre} (rows={len(out_a_pretty)})", flush=True)
    print(f"  - {p_f_raw} (rows={len(out_f_raw)})", flush=True)
    print(f"  - {p_f_pre} (rows={len(out_f_pretty)})", flush=True)

    # Console preview (first 15 rows, pretty)
    print("\nPreview (Actuals — first 15 rows):", flush=True)
    print(out_a_pretty.head(15).to_string(index=False), flush=True)

    print("\nPreview (Forecast — first 15 rows):", flush=True)
    print(out_f_pretty.head(15).to_string(index=False), flush=True)

    t_end(t0, "Total runtime")
    banner("DATA VALIDATION — DONE")

if __name__ == "__main__":
    main()