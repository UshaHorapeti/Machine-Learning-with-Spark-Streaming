
import subprocess
import tempfile
from pathlib import Path
import pandas as pd

SRC = r"C:\Users\11005092\BD\Power BI Tools Datasets - S&OP and Demand Waterfall\Downloads\Monthly Latest Snapshot\FORECAST_LIVE\SAP GERS.xlsx"
SHEET = "ZANALYSIS_PATTERN_WIDE"

def export_sheet_via_vbs(src_path: str, sheet_name: str, fmt: str = "xlsx") -> Path:
    """
    Use VBScript + Excel to copy the given sheet to a new workbook and save as .xlsx or .csv.
    Returns the path of the exported file.
    """
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

    Set ws = Nothing
    On Error Resume Next
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

    vbs_path = temp_dir / "export_sheet.vbs"
    vbs_path.write_text(vbs_code, encoding="utf-8")

    # Run VBScript via cscript (built into Windows)
    result = subprocess.run(
        ["cscript", "//nologo", str(vbs_path), src_path, sheet_name, str(dest), str(file_format)],
        capture_output=True, text=True
    )
    msg = (result.stdout or "").strip()
    if result.returncode != 0 or "OK" not in msg:
        raise RuntimeError(f"Excel export failed: {msg or result.stderr}")

    return dest

# --- Use the exporter, then read with pandas ---
try:
    # Export just the target sheet to a clean .xlsx
    exported = export_sheet_via_vbs(SRC, SHEET, fmt="xlsx")
    df = pd.read_excel(exported)   # pandas will handle this .xlsx fine
    print("Loaded rows:", len(df))
    print(df.head())
except Exception as e:
    # If .xlsx export fails, try CSV as fallback
    print("xlsx export failed, trying CSV. Reason:", e)
    exported = export_sheet_via_vbs(SRC, SHEET, fmt="csv")
    df = pd.read_csv(exported, encoding="utf-8", engine="python")
    print("Loaded rows:", len(df))
    print(df.head())
