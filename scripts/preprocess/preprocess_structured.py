"""
Preprocess: structured / medical data (CosIng + openFDA).

- standardize ingredient naming (INCI vs common name synonym table)
- handle missing function/status fields
- remove duplicate ingredient entries
- outlier checks on adverse event data (implausible dates, duplicate report IDs)
- feature selection: keep regulatory status + concentration limits, drop
  administrative metadata that doesn't feed the risk score
"""
import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import STAGING_DIR, RAW_STRUCTURED, PROCESSED_DIR, get_logger

log = get_logger("preprocess_structured")

# Minimal starter synonym table: common/marketing name -> INCI name.
# This is the "genuinely fiddly" part flagged in the original plan --
# expand this as you find more Open Beauty Facts ingredient strings that
# don't match CosIng directly.
SYNONYMS = {
    "vitamin c": "ascorbic acid",
    "vitamin b3": "niacinamide",
    "tranexamic acid": "trans-4-aminomethylcyclohexanecarboxylic acid",
    "alpha arbutin": "arbutin",
    "aha": "glycolic acid",
    "bha": "salicylic acid",
}

KEPT_COSING_COLUMNS = ["inci_name", "function", "regulatory_status"]  # + concentration_limit if present


def standardize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.strip().lower()
    return SYNONYMS.get(n, n)


def preprocess_cosing() -> pd.DataFrame:
    staged_path = STAGING_DIR / "cosing_staged.parquet"
    if not staged_path.exists():
        log.error("cosing_staged.parquet not found -- run extract_cosing.py first.")
        return pd.DataFrame()

    df = pd.read_parquet(staged_path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    keep_cols = [c for c in KEPT_COSING_COLUMNS + ["concentration_limit"] if c in df.columns]
    df = df[keep_cols].copy()

    n_missing_status = df["regulatory_status"].isna().sum() if "regulatory_status" in df else 0
    df["regulatory_status"] = df.get("regulatory_status", pd.Series(dtype=str)).fillna("unknown")
    df["function"] = df.get("function", pd.Series(dtype=str)).fillna("unspecified")

    df["inci_name_std"] = df["inci_name"].apply(standardize_name)
    before = len(df)
    df = df.drop_duplicates(subset=["inci_name_std"])
    log.info("CosIng: dropped %d duplicate ingredient rows", before - len(df))
    log.info("CosIng: %d rows had missing regulatory_status (filled as 'unknown')", n_missing_status)

    return df


def preprocess_openfda() -> pd.DataFrame:
    records = []
    for path in RAW_STRUCTURED.glob("openfda_*.json"):
        try:
            records.extend(json.loads(path.read_text()))
        except Exception:
            continue

    if not records:
        log.warning("No openFDA records found to preprocess.")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # outlier check: implausible dates (future dates, or absurdly old ones)
    if "receivedate" in df.columns:
        df["receivedate_parsed"] = pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce")
        today = pd.Timestamp(datetime.now())
        n_future = (df["receivedate_parsed"] > today).sum()
        n_unparseable = df["receivedate_parsed"].isna().sum()
        log.info("openFDA: %d records with future dates, %d unparseable dates", n_future, n_unparseable)
        df = df[df["receivedate_parsed"] <= today]

    # outlier check: duplicate report IDs
    id_col = "safetyreportid" if "safetyreportid" in df.columns else None
    if id_col:
        before = len(df)
        df = df.drop_duplicates(subset=[id_col])
        log.info("openFDA: dropped %d duplicate report IDs", before - len(df))

    return df


def main():
    cosing_df = preprocess_cosing()
    if not cosing_df.empty:
        cosing_df.to_parquet(PROCESSED_DIR / "cosing_processed.parquet", index=False)
        log.info("Saved %d processed CosIng rows", len(cosing_df))

    openfda_df = preprocess_openfda()
    if not openfda_df.empty:
        # coerce all object columns to string to avoid mixed-type pyarrow errors
        for col in openfda_df.select_dtypes(include="object").columns:
            openfda_df[col] = openfda_df[col].astype(str)
        openfda_df.to_parquet(PROCESSED_DIR / "openfda_processed.parquet", index=False)
        log.info("Saved %d processed openFDA rows", len(openfda_df))


if __name__ == "__main__":
    main()
