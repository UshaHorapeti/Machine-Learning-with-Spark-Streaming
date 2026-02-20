
from __future__ import annotations
import subprocess
import tempfile
import pandas as pd
from pathlib import Path
import glob
import re
import shutil
import time
from zipfile import BadZipFile
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

# ------------- Logging -------------
def log(msg: str) -> None:
    print(msg, flush=True)

# ------------- PowerShell -------------
def _get_powershell_exe() -> str:
    """
    Prefer 64-bit PowerShell. From a 32-bit Python process, System32 is redirected
    to SysWOW64. SysNative lets us reach the real 64-bit System32 on 64-bit Windows.
    """
    ps64 = r"C:\Windows\SysNative\WindowsPowerShell\v1.0\powershell.exe"
    ps32 = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
    return ps64 if Path(ps64).exists() else ps32

def run_powershell(ps_path: Path) -> None:
   
    if not ps_path.exists():
        raise FileNotFoundError(f"PowerShell script not found: {ps_path}")
    log(f"▶ Running PowerShell: {ps_path}")

    ps_exe = _get_powershell_exe()        
    log(f"ℹ Using PowerShell: {ps_exe}")

    result = subprocess.run(
        [
            ps_exe,
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", str(ps_path),
        ],
        cwd=str(ps_path.parent),             
        capture_output=True,
        text=True
    )
    

    if result.returncode != 0:
        raise RuntimeError(f"PowerShell failed with exit code {result.returncode}")
    log("✔ PowerShell completed.")

def _parse_date_from_filename(path: Path, regex: re.Pattern) -> Optional[datetime]:
    m = regex.search(path.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d")
    except Exception:
        return None

def find_latest_by_pattern(pattern: str, prefer_filename_date: bool = True) -> Path:
    files = [Path(p) for p in glob.glob(pattern)]
    if not files:
        raise FileNotFoundError(f"No file found matching pattern: {pattern}")
    if prefer_filename_date:
        date_regex = re.compile(r"^(\d{8})_?")
        dated: List[tuple[datetime, Path]] = []
        for f in files:
            dt = _parse_date_from_filename(f, date_regex)
            if dt:
                dated.append((dt, f))
        if dated:
            dated.sort(key=lambda x: x[0], reverse=True)
            selected = dated[0][1]
            log(f"📄 Selected latest by filename date: {selected}")
            return selected
    selected = max(files, key=lambda f: f.stat().st_mtime)
    log(f"📄 Selected latest by modified time: {selected}")
    return selected

def load_mapping(mapping_path: Path, sheet_name: str) -> pd.DataFrame:
    if not mapping_path.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_path}")
    df_map = pd.read_excel(mapping_path, sheet_name=sheet_name, dtype=str)
    log(f"✔ Loaded mapping: {df_map.shape[0]} rows, {df_map.shape[1]} columns")
    return df_map

def read_table_auto(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in (".csv", ".txt"):
        return pd.read_csv(path, dtype=str)
    elif ext in (".xlsx", ".xlsm"):
        return pd.read_excel(path, dtype=str, engine="openpyxl")
    elif ext == ".xls":
        return pd.read_excel(path, dtype=str)  
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

# ------------- Date utilities -------------
def snapshot_today_first() -> str:
    return datetime.today().replace(day=1).strftime("%m/%d/%Y")

def convert_attribute_row(row: pd.Series) -> Optional[str]:
    month_year = str(row.get("Calendar[Month Year]", "")).strip()
    month_sort = str(row.get("Calendar[Month Sort]", "")).strip()
    if month_year:
        for fmt in ("%b-%y", "%b %Y"):
            try:
                dt = datetime.strptime(month_year, fmt).replace(day=1)
                return dt.strftime("%m/%d/%Y")
            except Exception:
                continue
    if month_sort and len(month_sort) == 6 and month_sort.isdigit():
        try:
            year = int(month_sort[:4])
            month = int(month_sort[4:])
            dt = datetime(year, month, 1)
            return dt.strftime("%m/%d/%Y")
        except Exception:
            pass
    return None

def convert_mmYYYY_to_attribute(val: str) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    for fmt in ("%m/%Y", "%m-%Y"):
        try:
            dt = datetime.strptime(s, fmt).replace(day=1)
            return dt.strftime("%m/%d/%Y")
        except Exception:
            continue
    try:
        parts = s.replace("-", "/").split("/")
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            month = int(parts[0])
            year = int(parts[1])
            dt = datetime(year, month, 1)
            return dt.strftime("%m/%d/%Y")
    except Exception:
        pass
    return None

# ------------- Column & text utilities -------------
def pick_col(df: pd.DataFrame, candidates: List[str], required: bool = True) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    if required:
        raise KeyError(f"Missing required column(s): {candidates}")
    return None

def clean_country_performance(df: pd.DataFrame, country_col: str) -> None:
    if country_col in df.columns:
        df[country_col] = df[country_col].astype(str).str.replace(" Performance", "", regex=False)

def strip_units_to_number(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"[^0-9.\-]", "", str(s)).strip()

# ------------- JobConfig -------------
@dataclass
class JobConfig:
    label: str
    ps_path: Optional[Path]
    input_pattern: str
    output_path: Path
    use_mapping: bool = False
    snapshot_mode: str = "current_month"  
    sales_org_mode: str = "from_file"     
    skip_ps: bool = False
    source_const: Optional[str] = None
    source_col: Optional[str] = None
    snapshot_col: Optional[str] = None
    material_candidates: List[str] = field(default_factory=lambda: ["SIOP[Material ID Harmonized]", "SIOP[Material ID]"])
    value_col: str = "[SIOP f/Planning]"
    country_from: str = "corrected"
    bu_const: Optional[str] = None
    bu_col: Optional[str] = None
    country_clean_performance: bool = False

# ------------- Mapping merge -------------
def enrich_with_mapping(src_df: pd.DataFrame, map_df: pd.DataFrame) -> pd.DataFrame:
    key_cols = [
        "SIOP[Planning System]",
        "SIOP[Sales Organization]",
        "SIOP[Region]",
        "SIOP[Mapped Country]",
        "SIOP[Sub Region]",
    ]
    required_cols = key_cols + ["Corrected country"]
    for col in required_cols:
        if col not in map_df.columns:
            raise KeyError(f"Mapping file missing column: {col}")
    for col in key_cols:
        if col not in src_df.columns:
            raise KeyError(f"Source file missing column: {col}")
    merged = src_df.merge(map_df[required_cols], on=key_cols, how="left")
    log(f"🔗 Merge complete: {merged.shape[0]} rows")
    blanks = merged["SIOP[Country]"].isna() | (merged["SIOP[Country]"].astype(str).str.strip() == "")
    merged.loc[blanks, "SIOP[Country]"] = merged.loc[blanks, "Corrected country"]
    return merged

# ------------- Transform to final (generic feeds) -------------
def transform_to_final(df: pd.DataFrame, cfg: JobConfig, mapping_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Transform a raw feed to the standard final schema:
    ['Source', 'Snapshot', 'Material', 'Sales Organization', 'Country', 'Attribute', 'Value', 'BU']
    Behavior:
    - If use_mapping=True: merge mapping and use 'Corrected country' as the Country source.
    - Snapshot: current month (first day) unless snapshot_mode='from_file'.
    - Sales Org: blank when sales_org_mode='blank', otherwise from SIOP[Sales Organization] if available.
    - BU: from bu_const (if provided) else from bu_col/SIOP[ReltioBU].
    - When use_mapping=True, rewrite BU to "BLNK-C-<original>" or "BLNK-C-" if blank.
    """
    working = df.copy()
    if cfg.use_mapping:
        if mapping_df is None:
            raise ValueError("Mapping DataFrame must be supplied when use_mapping=True.")
        working = enrich_with_mapping(working, mapping_df)
        country_source_col = "Corrected country"
    else:
        country_source_col = cfg.country_from
    if country_source_col not in working.columns:
        raise KeyError(f"Country column '{country_source_col}' not found in source file.")

    if cfg.snapshot_mode == "current_month":
        working["Snapshot"] = snapshot_today_first()
    elif cfg.snapshot_mode == "from_file":
        snap_col = cfg.snapshot_col or "Snapshot"
        if snap_col not in working.columns:
            raise KeyError(f"Snapshot column '{snap_col}' required for snapshot_mode='from_file'.")

    working["Attribute"] = working.apply(convert_attribute_row, axis=1)
    if cfg.country_clean_performance:
        clean_country_performance(working, country_source_col)

    rename_map: Dict[str, str] = {}
    if cfg.source_const:
        working["Source"] = cfg.source_const
    elif cfg.source_col and cfg.source_col in working.columns:
        rename_map[cfg.source_col] = "Source"
    elif "SIOP[Planning System]" in working.columns:
        rename_map["SIOP[Planning System]"] = "Source"
    else:
        working["Source"] = cfg.label

    if cfg.snapshot_mode == "from_file":
        snap_col = cfg.snapshot_col or "Snapshot"
        rename_map[snap_col] = "Snapshot"

    material_src = pick_col(working, cfg.material_candidates, required=True)
    rename_map[material_src] = "Material"

    if cfg.sales_org_mode == "blank":
        working["Sales Organization"] = ""
    else:
        if "SIOP[Sales Organization]" in working.columns:
            rename_map["SIOP[Sales Organization]"] = "Sales Organization"
        else:
            working["Sales Organization"] = ""

    rename_map[country_source_col] = "Country"

    if cfg.value_col not in working.columns:
        raise KeyError(f"Value column '{cfg.value_col}' not found in source file.")
    rename_map[cfg.value_col] = "Value"

    if cfg.bu_const:
        working["BU"] = cfg.bu_const
    elif cfg.bu_col and cfg.bu_col in working.columns:
        rename_map[cfg.bu_col] = "BU"
    elif "SIOP[ReltioBU]" in working.columns:
        rename_map["SIOP[ReltioBU]"] = "BU"
    else:
        working["BU"] = ""

    working = working.rename(columns=rename_map)

    if cfg.use_mapping:
        if "BU" in working.columns:
            working["BU"] = (
                working["BU"].astype(str)
                .apply(lambda s: f"BLNK-C-{s.strip()}" if s.strip() else "BLNK-C-")
            )
        else:
            working["BU"] = "BLNK-C-()"

    final_cols = ["Source", "Snapshot", "Material", "Sales Organization", "Country", "Attribute", "Value", "BU"]
    missing = [c for c in final_cols if c not in working.columns]
    if missing:
        raise KeyError(f"Final columns missing: {missing}")
    return working[final_cols].copy()

# ------------- SAP GERS transform -------------
def transform_sap_gers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    def pick_duplicate(name: str, index: int) -> pd.Series:
        matches = [i for i, c in enumerate(df.columns) if c == name]
        if matches:
            return df.iloc[:, matches[index]]
        raise KeyError(f"Column '{name}' not found.")

    sales_org = pick_duplicate("Sales Organization", 0)
    matches_country = [i for i, c in enumerate(df.columns) if c == "Country"]
    if len(matches_country) >= 2:
        country = df.iloc[:, matches_country[1]]
    elif len(matches_country) == 1:
        country = df.iloc[:, matches_country[0]]
    else:
        raise KeyError("Country column(s) not found.")
    material = pick_duplicate("Material", 0)

    if "Cal. year / month" not in df.columns:
        raise KeyError("Column 'Cal. year / month' not found.")
    attribute = df["Cal. year / month"].apply(convert_mmYYYY_to_attribute)

    if "Actual/Forecast" not in df.columns:
        raise KeyError("Column 'Actual/Forecast' not found.")
    value = df["Actual/Forecast"].astype(str).apply(strip_units_to_number)

    out = pd.DataFrame({
        "Source": "SAP GERS",
        "Snapshot": snapshot_today_first(),
        "Material": material.astype(str).str.strip(),
        "Sales Organization": sales_org.astype(str).str.strip(),
        "Country": country.astype(str).str.strip(),
        "Attribute": attribute,
        "Value": value,
        "BU": "MDS",
    })
    return out

# ------------- Excel export via VBScript -------------
def export_sheet_via_vbs(src_path: str, sheet_name: str, fmt: str = "xlsx") -> Path:
    temp_dir = Path(tempfile.gettempdir())
    if fmt.lower() == "xlsx":
        dest = temp_dir / "SAP_GERS_export.xlsx"
        file_format = 51  # xlOpenXMLWorkbook (.xlsx)
    elif fmt.lower() == "csv":
        dest = temp_dir / "SAP_GERS_export.csv"
        file_format = 6   # xlCSV
    else:
        raise ValueError("fmt must be 'xlsx' or 'csv'")

    vbs_code = r'''
On Error Resume Next
Dim src, sheet, dest, fmt
src = WScript.Arguments(0)
sheet = WScript.Arguments(1)
dest = WScript.Arguments(2)
fmt = CInt(WScript.Arguments(3))
Dim xl, wb, ws, wb2
Set xl = CreateObject("Excel.Application")
xl.DisplayAlerts = False
Set wb = xl.Workbooks.Open(src)
If Err.Number <> 0 Then
  WScript.Echo "OPEN_FAIL:" & Err.Description
  xl.Quit
  WScript.Quit 1
End If
Set ws = wb.Worksheets(sheet)
If ws Is Nothing Then
  WScript.Echo "SHEET_NOT_FOUND"
  wb.Close False
  xl.Quit
  WScript.Quit 2
End If
ws.Copy
Set wb2 = xl.ActiveWorkbook
wb2.SaveAs dest, fmt
wb2.Close False
wb.Close False
xl.Quit
WScript.Echo "OK"
WScript.Quit 0
'''
    temp_dir.mkdir(parents=True, exist_ok=True)
    vbs_path = temp_dir / "export_sheet.vbs"
    vbs_path.write_text(vbs_code, encoding="utf-8")

    result = subprocess.run(
        ["cscript", "//nologo", str(vbs_path), src_path, sheet_name, str(dest), str(file_format)],
        capture_output=True, text=True
    )
    msg = (result.stdout or "").strip()
    if result.returncode != 0 or "OK" not in msg:
        raise RuntimeError(f"Excel export failed: {msg or result.stderr}")
    return dest

# ------------- Header normalization -------------
def _detect_header_row(df_raw: pd.DataFrame, max_scan_rows: int = 10) -> int:
    n = min(max_scan_rows, len(df_raw))
    for i in range(n):
        vals = df_raw.iloc[i].astype(str).str.strip().tolist()
        lower = [v.lower() for v in vals]
        if ("cal. year / month" in lower) and ("actual/forecast" in lower):
            return i
        if ("sales organization" in lower) and ("country" in lower):
            return i
    return 2  

def normalize_sheet_with_header_row(df_raw: pd.DataFrame) -> pd.DataFrame:
    header_idx = _detect_header_row(df_raw)
    header = df_raw.iloc[header_idx].astype(str).str.strip()
    df = df_raw.iloc[header_idx + 1:].copy()
    df.columns = header.values
    df = df.loc[:, df.notna().any(axis=0)]
    df = df.dropna(how="all").reset_index(drop=True)
    return df.apply(lambda col: col.astype(str).str.strip())

def run_job(cfg: JobConfig, mapping_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    if not cfg.skip_ps:
        if cfg.ps_path is None:
            raise ValueError(f"Job '{cfg.label}' requires ps_path unless skip_ps=True.")
        run_powershell(cfg.ps_path)

    in_path = find_latest_by_pattern(cfg.input_pattern, prefer_filename_date=True)

    df_src = read_table_auto(in_path)
    log(f"✔ Loaded {cfg.label} data: {df_src.shape[0]} rows, {df_src.shape[1]} columns")

    df_final = transform_to_final(df_src, cfg, mapping_df=mapping_df)

    cfg.output_path.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(cfg.output_path, index=False, encoding="utf-8")
    log(f"💾 {cfg.label}: Saved transformed file -> {cfg.output_path}")

    return {
        "label": cfg.label,
        "input": str(in_path),
        "output": str(cfg.output_path),
        "rows_in": int(df_src.shape[0]),
        "rows_out": int(df_final.shape[0]),
    }

# ------------- SAP GERS runner  -------------
def run_job_sap_gers_vbs(src_path: Path, sheet_name: str, output_path: Path, label: str = "SAP GERS") -> Dict[str, Any]:
    log(f"▶ {label}: Exporting sheet '{sheet_name}' from {src_path}")
    try:
        exported = export_sheet_via_vbs(str(src_path), sheet_name, fmt="xlsx")
        log(f"✔ Exported XLSX: {exported}")
        df_raw = pd.read_excel(exported, header=None, dtype=str, engine="openpyxl")
    except Exception as e_xlsx:
        log(f"ℹ XLSX export/load failed ({e_xlsx}); trying CSV fallback.")
        exported = export_sheet_via_vbs(str(src_path), sheet_name, fmt="csv")
        log(f"✔ Exported CSV: {exported}")
        df_raw = pd.read_csv(exported, header=None, dtype=str, encoding="utf-8", engine="python")

    df_norm = normalize_sheet_with_header_row(df_raw)
    log(f"✅ Loaded {label} normalized data: {df_norm.shape[0]} rows, {df_norm.shape[1]} columns")

    df_final = transform_sap_gers(df_norm)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(output_path, index=False, encoding="utf-8")
    log(f"💾 {label}: Saved transformed file -> {output_path}")

    return {
        "label": label,
        "input": str(src_path),
        "output": str(output_path),
        "rows_in": int(df_norm.shape[0]),
        "rows_out": int(df_final.shape[0]),
    }
