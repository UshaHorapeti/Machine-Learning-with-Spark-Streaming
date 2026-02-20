
#!/usr/bin/env python
import sys
import time
from pathlib import Path
import numpy as np
import pandas as pd

# Ensure Python can see your internal package (mySQLClass)
sys.path.insert(0, r"C:\DP Waterfall Automation")
sys.path.insert(0, r"C:\DP Waterfall Automation\packages")
from mySQLClass import mySQLDB

# -------------------- CONFIG --------------------
SQL_UID = "11005077"
SQL_PWD = "Test!123Test!123"
AUTH_TYPE = "DB"

TABLE_ACTUALS  = "dbo.Demand_Actuals"
TABLE_FORECAST = "dbo.Demand_Forecast"

OUTPUT_DIR = Path(r"F:\DP Waterfall\Validation")
QC_THRESHOLD = 0.15  # ±15%

# -------------------- HELPERS --------------------
def banner(msg: str):
    print(f"\n==== {msg} ====", flush=True)

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

def latest_two_snapshots(engine, table: str) -> tuple[str, str]:
    """
    Return (latest, previous) snapshot dates as 'mm/dd/yyyy' strings.
    Works whether [Snapshot] is DATE/DATETIME or VARCHAR.
    """
    sql = f"""
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    WITH snaps AS (
        SELECT TRY_CONVERT(date, [Snapshot]) AS SnapDate
        FROM {table} WITH (NOLOCK)
    )
    SELECT TOP (2)
           CONVERT(varchar(10), SnapDate, 101) AS SnapText
    FROM snaps
    WHERE SnapDate IS NOT NULL
    GROUP BY SnapDate
    ORDER BY SnapDate DESC;
    """
    df = pd.read_sql(sql, engine)
    snaps = [str(x).strip() for x in df["SnapText"].tolist() if pd.notna(x)]
    if len(snaps) < 2:
        raise RuntimeError(f"[{table}] Need at least two distinct snapshots; found {len(snaps)}.")
    return snaps[0], snaps[1]  # latest, previous

def read_df_for_snapshot(engine, table: str, snapshot_text: str) -> pd.DataFrame:
    """
    Read rows for the specified snapshot date text (mm/dd/yyyy),
    matching regardless of underlying column type.
    """
    snap_q = snapshot_text.replace("'", "''")
    sql = f"""
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SELECT Source, Snapshot, Material, [Sales Organization], Country, Attribute, Value, BU
    FROM {table} WITH (NOLOCK)
    WHERE TRY_CONVERT(date, [Snapshot]) = TRY_CONVERT(date, '{snap_q}');
    """
    df = pd.read_sql(sql, engine)
    df.columns = [str(c).strip() for c in df.columns]
    df["Country"] = df["Country"].astype(str).str.strip()
    df["BU"]      = df["BU"].astype(str).str.strip()
    df["Value"]   = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)
    return df

def aggregates(df: pd.DataFrame) -> dict:
    """
    Return totals by BU, Country, and BU|Country.
    """
    if df.empty:
        empty = pd.DataFrame(columns=["Key", "value_sum"])
        return {"BU": empty.copy(), "Country": empty.copy(), "BU+Country": empty.copy()}

    x = df.copy()
    x["Value"]   = pd.to_numeric(x["Value"], errors="coerce").fillna(0.0)
    x["Country"] = x["Country"].fillna("").astype(str).str.strip()
    x["BU"]      = x["BU"].fillna("").astype(str).str.strip()

    by_bu = (
        x.groupby("BU", dropna=False, sort=False)["Value"].sum()
         .reset_index().rename(columns={"BU": "Key", "Value": "value_sum"})
    )
    by_country = (
        x.groupby("Country", dropna=False, sort=False)["Value"].sum()
         .reset_index().rename(columns={"Country": "Key", "Value": "value_sum"})
    )
    x["BU_Country"] = x["BU"].str.strip() + " | " + x["Country"].str.strip()
    by_bucountry = (
        x.groupby("BU_Country", dropna=False, sort=False)["Value"].sum()
         .reset_index().rename(columns={"BU_Country": "Key", "Value": "value_sum"})
    )
    return {"BU": by_bu, "Country": by_country, "BU+Country": by_bucountry}

def compare_latest_vs_prev(engine, table: str, threshold=0.15) -> tuple[pd.DataFrame, str, str]:
    """
    Build comparison rows for latest vs previous snapshot.
    """
    snap_latest, snap_prev = latest_two_snapshots(engine, table)
    df_latest = read_df_for_snapshot(engine, table, snap_latest)
    df_prev   = read_df_for_snapshot(engine, table, snap_prev)

    agg_L = aggregates(df_latest)
    agg_P = aggregates(df_prev)

    rows = []
    for dim in ["BU", "Country", "BU+Country"]:
        L = agg_L[dim].set_index("Key")
        P = agg_P[dim].set_index("Key")
        keys = sorted(set(L.index) | set(P.index))
        for k in keys:
            vL = float(L.loc[k, "value_sum"]) if k in L.index else 0.0
            vP = float(P.loc[k, "value_sum"]) if k in P.index else 0.0

            if vP == 0.0:
                pct = 0.0 if vL == 0.0 else float("inf")
                qc  = "PASS" if vL == 0.0 else "FAIL"
            else:
                pct = (vL - vP) / vP
                qc  = "PASS" if abs(pct) <= threshold else "FAIL"

            rows.append({
                "Dimension":      dim,
                "Key":            k,
                "Latest snapshot":   snap_latest,
                "Previous snapshot": snap_prev,
                "Latest value":   vL,
                "Previous value": vP,
                "Pct diff":       pct,
                "QC":             qc
            })

    comp = pd.DataFrame(rows)
    comp["Pct diff"] = pd.to_numeric(comp["Pct diff"], errors="coerce")
    comp["__abs__"]  = comp["Pct diff"].astype("float64").abs()
    comp = (
        comp.sort_values(["Dimension", "__abs__"], ascending=[True, False])
            .drop(columns=["__abs__"])
            .reset_index(drop=True)
    )
    return comp, snap_latest, snap_prev

def format_for_output(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Latest value"]   = out["Latest value"].map(fmt_num)
    out["Previous value"] = out["Previous value"].map(fmt_num)
    out["Pct diff"]       = out["Pct diff"].map(fmt_pct)
    return out

def save_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[SAVED] {path}", flush=True)

# -------------------- MAIN --------------------
def main():
    t0 = time.time()
    banner("SQL CONNECT")
    db = mySQLDB(uid=SQL_UID, pwd=SQL_PWD, uid_type=AUTH_TYPE)

    banner("ACTUALS: LATEST vs PREVIOUS")
    comp_a, snapL_a, snapP_a = compare_latest_vs_prev(db.engine, TABLE_ACTUALS, threshold=QC_THRESHOLD)
    comp_a_fmt = format_for_output(comp_a)
    print(f"Snapshots -> Latest: {snapL_a} | Previous: {snapP_a}", flush=True)
    print("\n[Actuals preview (top 25)]:", flush=True)
    print(comp_a_fmt.head(25).to_string(index=False), flush=True)
    save_csv(comp_a_fmt, OUTPUT_DIR / "Actuals Latest vs Previous.csv")

    banner("FORECAST: LATEST vs PREVIOUS")
    comp_f, snapL_f, snapP_f = compare_latest_vs_prev(db.engine, TABLE_FORECAST, threshold=QC_THRESHOLD)
    comp_f_fmt = format_for_output(comp_f)
    print(f"Snapshots -> Latest: {snapL_f} | Previous: {snapP_f}", flush=True)
    print("\n[Forecast preview (top 25)]:", flush=True)
    print(comp_f_fmt.head(25).to_string(index=False), flush=True)
    save_csv(comp_f_fmt, OUTPUT_DIR / "Forecast Latest vs Previous.csv")

    banner("DONE")
    print(f"[TIME] Total runtime: {time.time() - t0:.2f} sec", flush=True)

if __name__ == "__main__":
    main()
