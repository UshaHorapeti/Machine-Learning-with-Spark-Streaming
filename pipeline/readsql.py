import sys
try:
    if r"C:\e2esc" in sys.path:
        sys.path.remove(r"C:\e2esc")
except Exception:
    pass

import os
import socket
import pandas as pd

SERVER   = os.getenv("SQLSERVER", "pw01tgsshsdb02.database.windows.net")
PORT     = int(os.getenv("SQLPORT", "1433"))
DATABASE = os.getenv("SQLDB",    "MS_Analytics")
USER     = os.getenv("SQLUSER",  "")     
PASSWORD = os.getenv("SQLPASS",  "")     

TABLE_ACTUALS  = os.getenv("TABLE_ACTUALS",  "dbo.Demand_Actuals")
TABLE_FORECAST = os.getenv("TABLE_FORECAST", "dbo.Demand_Forecast")

CONNECT_TIMEOUT = int(os.getenv("ODBC_CONN_TIMEOUT", "10"))
QUERY_TIMEOUT   = int(os.getenv("ODBC_QUERY_TIMEOUT", "300"))

def _tcp_reachable(host: str, port: int, timeout=4) -> bool:
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        return False
    import socket as s
    sock = s.socket(s.AF_INET, s.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((ip, port))
        return True
    except Exception:
        return False
    finally:
        sock.close()

def _pick_driver():
    import pyodbc
    drivers = [d.strip() for d in pyodbc.drivers()]
    for candidate in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server"):
        if candidate in drivers:
            return candidate
    raise RuntimeError(f"No SQL Server ODBC driver found. Available drivers: {drivers}")

def _connect_pyodbc():
    import pyodbc
    driver = _pick_driver()
    server_part = f"tcp:{SERVER},{PORT}"  
    parts = [
        f"Driver={{{driver}}}",
        f"Server={server_part}",
        f"Database={DATABASE}",
        "Encrypt=yes",
        "TrustServerCertificate=yes",  
    ]
    if USER and PASSWORD:
        parts += [f"UID={USER}", f"PWD={PASSWORD}"]
    else:
        parts += ["Trusted_Connection=yes"]
    conn_str = ";".join(parts)
    cn = pyodbc.connect(conn_str, timeout=CONNECT_TIMEOUT)
    cur = cn.cursor()
    try:
        cur.timeout = QUERY_TIMEOUT
    except Exception:
        pass
    cur.close()
    return cn

def _detect_latest_snapshot(cn, table: str) -> str:
    sql_max_date = f"""
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
        SELECT CONVERT(varchar(10), MAX(TRY_CONVERT(date, Snapshot)), 101) AS SnapshotText
        FROM {table};
    """
    df = pd.read_sql(sql_max_date, cn)
    snap = (str(df.iloc[0, 0]).strip() if not df.empty and pd.notna(df.iloc[0, 0]) else "")
    if not snap:
        sql_max_text = f"""
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            SELECT TOP (1) Snapshot
            FROM {table} WITH (NOLOCK)
            WHERE ISDATE(Snapshot) = 1
            ORDER BY TRY_CONVERT(date, Snapshot) DESC;
        """
        df2 = pd.read_sql(sql_max_text, cn)
        snap = (str(df2.iloc[0, 0]).strip() if not df2.empty and pd.notna(df2.iloc[0, 0]) else "")
    if not snap:
        raise RuntimeError(f"[{table}] Snapshot detection failed.")
    return snap

def _read_latest_snapshot_df(cn, table: str, snapshot_text: str) -> pd.DataFrame:
    sql = f"""
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
        SELECT Source, Snapshot, Material, [Sales Organization], Country, Attribute, Value, BU
        FROM {table} WITH (NOLOCK)
        WHERE TRY_CONVERT(date, Snapshot) = TRY_CONVERT(date, ?);
    """
    df = pd.read_sql(sql, cn, params=[snapshot_text])
    df.columns = [str(c).strip() for c in df.columns]
    df["Country"] = df["Country"].astype(str).str.strip()
    df["BU"] = df["BU"].astype(str).str.strip()
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0.0)
    return df

def get_latest_dataframes() -> dict:
    """
    Returns:
        {
          "actuals":  pandas.DataFrame (latest snapshot),
          "forecast": pandas.DataFrame (latest snapshot)
        }
    """
    if not _tcp_reachable(SERVER, PORT):
        raise RuntimeError(f"Cannot reach {SERVER}:{PORT}. Check VPN/firewall/server name.")
    cn = _connect_pyodbc()
    try:
        snap_a = _detect_latest_snapshot(cn, TABLE_ACTUALS)
        df_a   = _read_latest_snapshot_df(cn, TABLE_ACTUALS, snap_a)
        df_a.attrs["latest_snapshot"] = snap_a

        snap_f = _detect_latest_snapshot(cn, TABLE_FORECAST)
        df_f   = _read_latest_snapshot_df(cn, TABLE_FORECAST, snap_f)
        df_f.attrs["latest_snapshot"] = snap_f
    finally:
        try:
            cn.close()
        except Exception:
            pass
    return {"actuals": df_a, "forecast": df_f}
