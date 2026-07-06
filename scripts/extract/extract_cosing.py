"""
Extract: EU CosIng ingredient regulatory dataset.

Reads the official CosIng bulk export CSV (manually downloaded from
https://ec.europa.eu/growth/tools-databases/cosing/) and stages it as
parquet for downstream loading into the relational staging table.

Column mapping from official CosIng export → internal names:
  'INCI name'   → inci_name
  'Restriction' → regulatory_status
  'Function'    → function
"""
import sys
import pandas as pd
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import COSING_CSV_PATH, STAGING_DIR, get_logger

log = get_logger("extract_cosing")


def main():
    if not COSING_CSV_PATH.exists():
        log.error(
            "CosIng export not found at %s.\n"
            "Download from https://ec.europa.eu/growth/tools-databases/cosing/ "
            "and save as CSV at that path, then re-run.",
            COSING_CSV_PATH,
        )
        return

    df = pd.read_csv(COSING_CSV_PATH, encoding="latin-1")

    # Rename official CosIng columns → internal standard names
    df = df.rename(columns={
        "INCI name":   "inci_name",
        "Restriction": "regulatory_status",
        "Function":    "function",
        "CAS No":      "cas_no",
        "EC No":       "ec_no",
        "COSING Ref No": "cosing_ref_no",
        "Chem/IUPAC Name / Description": "chem_description",
        "Update Date": "update_date",
    })

    # Lowercase all column names for consistency
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    log.info("Loaded %d CosIng rows, columns: %s", len(df), df.columns.tolist())

    # Basic quality check
    missing_core = {"inci_name", "regulatory_status", "function"} - set(df.columns)
    if missing_core:
        log.warning("Still missing expected columns %s — check rename mapping", missing_core)

    null_counts = df[["inci_name", "regulatory_status", "function"]].isnull().sum()
    
    log.info("Null counts in core columns:\n%s", null_counts.to_string())

    staged_path = STAGING_DIR / "cosing_staged.parquet"
    df.to_parquet(staged_path, index=False)
    log.info("Staged %d CosIng rows -> %s", len(df), staged_path)
    log.info("Next: run scripts/etl/load.py to push into staging RDBMS table.")


if __name__ == "__main__":
    main()