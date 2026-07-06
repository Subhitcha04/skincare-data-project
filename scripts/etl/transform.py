"""
Transform: the core ETL "T" step.

- standardize ingredient names across OBF + CosIng (reuses the synonym table)
- unify date formats across PubMed / openFDA / YouTube
- join OBF ingredient lists against CosIng regulatory status -> per-product risk flags
- aggregate to:
    * product-level risk_score   (avg/max across its flagged ingredients)
    * source-level evidence_score (# PubMed hits per claim/ingredient)
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
sys.path.append(str(Path(__file__).resolve().parents[1] / "preprocess"))
from config import PROCESSED_DIR, get_logger
from preprocess_structured import standardize_name  # reuse the same synonym logic  # noqa

log = get_logger("transform")

# crude regulatory_status -> numeric risk weight; tune based on your rubric
RISK_WEIGHTS = {
    "restricted": 0.8,
    "banned": 1.0,
    "allowed": 0.1,
    "unknown": 0.4,
}


def unify_dates():
    """Light helper: call pd.to_datetime with explicit formats per source so
    PubMed pub dates / openFDA receivedates / YouTube publishedAt all land in
    the same dtype before any join or time-based aggregation."""
    pass  # each preprocess_*.py already normalizes its own date column;
          # this is the single place to add cross-source date alignment logic
          # if/when you start joining on time windows.


def compute_product_risk(images_df: pd.DataFrame, cosing_df: pd.DataFrame) -> pd.DataFrame:
    cosing_lookup = cosing_df.set_index("inci_name_std")["regulatory_status"].to_dict()

    def score_row(ocr_text: str) -> float:
        if not isinstance(ocr_text, str) or not ocr_text:
            return np.nan
        tokens = [standardize_name(t) for t in ocr_text.replace("\n", ",").split(",") if t.strip()]
        weights = [RISK_WEIGHTS.get(cosing_lookup.get(t, "unknown"), 0.4) for t in tokens]
        return float(np.mean(weights)) if weights else np.nan

    images_df = images_df.copy()
    images_df["risk_score"] = images_df["ocr_text"].apply(score_row)
    return images_df


def compute_evidence_scores(pubmed_df: pd.DataFrame) -> pd.DataFrame:
    """Evidence-support score per ingredient term = normalized PubMed hit count."""
    counts = pubmed_df.groupby("ingredient_term").size().rename("pubmed_hits").reset_index()
    max_hits = counts["pubmed_hits"].max() or 1
    counts["evidence_score"] = counts["pubmed_hits"] / max_hits
    return counts


def main():
    cosing_path = PROCESSED_DIR / "cosing_processed.parquet"
    images_path = PROCESSED_DIR / "images_processed.parquet"
    pubmed_path = PROCESSED_DIR / "pubmed_processed.parquet"

    if not (cosing_path.exists() and images_path.exists()):
        log.error("Need cosing_processed.parquet and images_processed.parquet -- run preprocessing first.")
        return

    cosing_df = pd.read_parquet(cosing_path)
    images_df = pd.read_parquet(images_path)

    product_risk = compute_product_risk(images_df, cosing_df)
    product_risk.to_parquet(PROCESSED_DIR / "product_risk_scores.parquet", index=False)
    log.info("Saved per-product risk scores -> product_risk_scores.parquet")

    if pubmed_path.exists():
        pubmed_df = pd.read_parquet(pubmed_path)
        evidence = compute_evidence_scores(pubmed_df)
        evidence.to_parquet(PROCESSED_DIR / "evidence_scores.parquet", index=False)
        log.info("Saved evidence-support scores -> evidence_scores.parquet")
    else:
        log.warning("pubmed_processed.parquet missing -- skipping evidence score step.")


if __name__ == "__main__":
    main()
