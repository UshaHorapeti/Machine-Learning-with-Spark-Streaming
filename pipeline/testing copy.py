
#!/usr/bin/env python
# Sum(Value) for Actuals & Forecast from CSVs (NO snapshot filtering)

from pathlib import Path
import pandas as pd

# --- Config ---
EXTRACTED_DIR = Path(r"F:\DP Waterfall\Transformed")

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

def read_concat(file_list):
    """Read a list of CSVs from EXTRACTED_DIR, ensure schema, and return a concatenated DataFrame."""
    frames = []
    for fname in file_list:
        path = EXTRACTED_DIR / fname
        if not path.exists():
            print(f"[WARN] Missing: {fname}")
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as e:
            print(f"[ERROR] Could not read {fname}: {e}")
            continue

        miss = [c for c in EXPECTED_COLS if c not in df.columns]
        if miss:
            print(f"[SKIP] {fname} missing columns {miss}")
            continue

        # Keep only expected columns; coerce Value to numeric for summation
        df = df[EXPECTED_COLS].copy()
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)

        frames.append(df)
        print(f"[OK] {fname} rows={len(df)} sum(Value)={df['Value'].sum():,.2f}")

    if not frames:
        return pd.DataFrame(columns=EXPECTED_COLS)
    return pd.concat(frames, ignore_index=True)

def main():
    print("=== Summing Value across ALL rows (no snapshot filtering) ===")

    print("\n-- Actuals --")
    df_a = read_concat(ACTUAL_FILES)
    sum_a = float(df_a["Value"].sum()) if not df_a.empty else 0.0

    print("\n-- Forecast --")
    df_f = read_concat(FORECAST_FILES)
    sum_f = float(df_f["Value"].sum()) if not df_f.empty else 0.0

    combined = sum_a + sum_f

    fmt = lambda x: f"{x:,.2f}"
    print("\n=== RESULTS (All rows) ===")
    print(f"Actuals  SUM(Value): {fmt(sum_a)}")
    print(f"Forecast SUM(Value): {fmt(sum_f)}")
    print(f"Combined SUM(Value): {fmt(combined)}")

    # Optional: counts for visibility
    print("\n(Info) Row counts (all rows, unfiltered):")
    print(f"Actuals  rows: {len(df_a)}")
    print(f"Forecast rows: {len(df_f)}")

if __name__ == "__main__":
    main()
