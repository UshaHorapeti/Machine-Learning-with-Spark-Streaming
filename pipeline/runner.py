
import sys
from pathlib import Path
from lib import (
    JobConfig, load_mapping, run_job, run_job_sap_gers_vbs, log
)

# ---- Paths ----
MAPPING_FILE = Path(r"C:\Users\TEMP\BD\Power BI Tools Datasets - S&OP and Demand Waterfall\Key Region Mapping.xlsx")
MAPPING_SHEET = "Main"
CSV_FOLDER = Path(r"F:\DP Waterfall")
OUT_FOLDER = CSV_FOLDER / "Transformed"
PS_DIR = Path(r"C:\DP Waterfall Automation\src")

# ---- SAP GERS workbook & sheet ----
SAPGERS_PATH = Path(r"C:\Users\TEMP\BD\Power BI Tools Datasets - S&OP and Demand Waterfall\Downloads\Monthly Latest Snapshot\FORECAST_LIVE\SAP GERS.xlsx")
SAPGERS_SHEET = "ZANALYSIS_PATTERN_WIDE"

def jc(**kwargs) -> JobConfig:
    return JobConfig(**kwargs)

def build_jobs():
    return [
        # === 1) ACT Blank  ===
        jc(
            label="ACT",
            ps_path=PS_DIR / "ActDemandBlank.ps1",
            input_pattern=str(CSV_FOLDER / "*_ActDB.csv"),
            output_path=OUT_FOLDER / "ACT_DemandBlank_Transformed.csv",
            use_mapping=True,
            snapshot_mode="current_month",
            sales_org_mode="from_file",
            material_candidates=["SIOP[Material ID Harmonized]", "SIOP[Material ID]"],
            value_col="[SIOP f/Planning]",
            country_from="corrected",
            bu_col="SIOP[ReltioBU]",
            skip_ps=False,
        ),

        # === 2) FCST Blank  ===
        jc(
            label="FCST",
            ps_path=PS_DIR / "FCST_DemandBlank.ps1",
            input_pattern=str(CSV_FOLDER / "*_FcstDB.csv"),
            output_path=OUT_FOLDER / "FCST_DemandBlank_Transformed.csv",
            use_mapping=True,
            snapshot_mode="current_month",
            sales_org_mode="from_file",
            material_candidates=["SIOP[Material ID Harmonized]", "SIOP[Material ID]"],
            value_col="[SIOP f/Planning]",
            country_from="corrected",
            bu_col="SIOP[ReltioBU]",
            skip_ps=False,
        ),

        # === 3) ACT NonBlank 1 ===
        jc(
            label="ACT_DNB1",
            ps_path=PS_DIR / "ACT_DemandNonBlank1.ps1",
            input_pattern=str(CSV_FOLDER / "*_ActDNB1.csv"),
            output_path=OUT_FOLDER / "ACT_DemandNonBlank1_Transformed.csv",
            use_mapping=False,
            snapshot_mode="current_month",
            sales_org_mode="from_file",
            material_candidates=["SIOP[Material ID Harmonized]", "SIOP[Material ID]"],
            value_col="[SIOP f/Planning]",
            country_from="SIOP[Country]",
            bu_col="SIOP[ReltioBU]",
            skip_ps=False,
        ),

        # === 4) ACT NonBlank 2 ===
        jc(
            label="ACT_DNB2",
            ps_path=PS_DIR / "ACT_DemandNonBlank2.ps1",
            input_pattern=str(CSV_FOLDER / "*_ActDNB2.csv"),
            output_path=OUT_FOLDER / "ACT_DemandNonBlank2_Transformed.csv",
            use_mapping=False,
            snapshot_mode="current_month",
            sales_org_mode="from_file",
            material_candidates=["SIOP[Material ID Harmonized]", "SIOP[Material ID]"],
            value_col="[SIOP f/Planning]",
            country_from="SIOP[Country]",
            bu_col="SIOP[ReltioBU]",
            skip_ps=False,
        ),

        # === 5) FCST NonBlank 1..7 ===
        *[
            jc(
                label=f"FCST_DNB{i}",
                ps_path=PS_DIR / f"FCST_DemandNonBlank{i}.ps1",
                input_pattern=str(CSV_FOLDER / f"*_Fcst{i}.csv"),
                output_path=OUT_FOLDER / f"FCST_DemandNonBlank{i}_Transformed.csv",
                use_mapping=False,
                snapshot_mode="current_month",
                sales_org_mode="from_file",
                material_candidates=["SIOP[Material ID Harmonized]", "SIOP[Material ID]"],
                value_col="[SIOP f/Planning]",
                country_from="SIOP[Country]",
                bu_col="SIOP[ReltioBU]",
                skip_ps=False,
            )
            for i in range(1, 8)
        ],

        # === 6) BPC VAD ===
        jc(
            label="BPC_VAD",
            ps_path=PS_DIR / "BPC_VAD.ps1",
            input_pattern=str(CSV_FOLDER / "*Bpcvad.csv"), 
            output_path=OUT_FOLDER / "BPC_VAD_Transformed.csv",
            use_mapping=False,
            snapshot_mode="current_month",
            sales_org_mode="blank",
            material_candidates=["BPC All[Material ID]"],
            value_col="[BPC Only - Actual Units]",
            country_from="BPC All[Country]",
            bu_const="MDS",
            country_clean_performance=True,
            source_const="BPC",
            skip_ps=False,
        ),

        # === 7) BPC by SKU ===
        jc(
            label="BPC_bySKU",
            ps_path=PS_DIR / "BPCbySKU.ps1",
            input_pattern=str(CSV_FOLDER / "*_Bpcbysku.csv"),
            output_path=OUT_FOLDER / "BPC_bySKU_Transformed.csv",
            use_mapping=False,
            snapshot_mode="current_month",
            sales_org_mode="blank",
            material_candidates=["BPC All[Material ID]"],
            value_col="[BPC Actuals]",
            country_from="BPC All[Country]",
            bu_const="MDS",
            country_clean_performance=True,
            source_const="BPC",
            skip_ps=False,
        ),

        # === 8) ACT Unknown  ===
        jc(
            label="ACT_Unknown",
            ps_path=PS_DIR / "ActUnknown.ps1",
            input_pattern=str(CSV_FOLDER / "*ActUnknown.csv"),
            output_path=OUT_FOLDER / "ACT_Unknown_Transformed.csv",
            use_mapping=False,
            snapshot_mode="current_month",           
            snapshot_col="SIOP[Snapshot Date]",  
            sales_org_mode="from_file",
            material_candidates=["SIOP[Material ID Harmonized]", "SIOP[Material ID]"],
            value_col="SIOP[SIOP Consensus]",
            country_from="SIOP[Country]",
            source_col="SIOP[Planning System]",
            bu_col="SIOP[ReltioBU]",
            skip_ps=False,
        ),

        # === 9) FCST Unknown ===
        jc(
            label="FCST_Unknown",
            ps_path=PS_DIR / "FcstUnknown.ps1",
            input_pattern=str(CSV_FOLDER / "*FcstUnknown.csv"),
            output_path=OUT_FOLDER / "FCST_Unknown_Transformed.csv",
            use_mapping=False,
            snapshot_mode="current_month",           
            snapshot_col="SIOP[Snapshot Date]",  
            sales_org_mode="from_file",
            material_candidates=["SIOP[Material ID Harmonized]", "SIOP[Material ID]"],
            value_col="SIOP[SIOP Consensus]",
            country_from="SIOP[Country]",
            source_col="SIOP[Planning System]",
            bu_col="SIOP[ReltioBU]",
            skip_ps=False,
        ),
    ]

def main():
    summaries = []

    # ---- Load mapping - for req files ----
    jobs = build_jobs()
    mapping_df = None
    if any(j.use_mapping for j in jobs):
        try:
            mapping_df = load_mapping(MAPPING_FILE, MAPPING_SHEET)
        except Exception as e:
            log(f"❌ Mapping load failed: {e}")
            sys.exit(1)

    # ---- Run all PowerShell in seq----
    for job in jobs:
        log(f"\n=== {job.label}: Starting ===")
        try:
            summary = run_job(job, mapping_df=mapping_df)
            summaries.append(summary)
            log(f"=== {job.label}: Complete ===")
        except Exception as e:
            log(f"❌ {job.label}: Error -> {e}")
            sys.exit(1)  # fail-fast

    # ---- SAP GERS (VBScript export path) ----
    try:
        log("\n=== SAP GERS: Starting (Excel export) ===")
        summary_gers = run_job_sap_gers_vbs(
            src_path=SAPGERS_PATH,
            sheet_name=SAPGERS_SHEET,
            output_path=OUT_FOLDER / "SAP_GERS_Transformed.csv",
            label="SAP GERS",
        )
        summaries.append(summary_gers)
        log("=== SAP GERS: Complete ===")
    except Exception as e:
        log(f"❌ SAP GERS: Error -> {e}")
        sys.exit(1)  # fail-fast

    # ---- Summary ----
    log("\n📊 Run summary:")
    for s in summaries:
        log(f" - {s['label']}: in={s['rows_in']} out={s['rows_out']} file={s['output']}")

if __name__ == "__main__":
    main()
