"""
EDA Report: cross-source summary.
Reads already-processed parquet files — does NOT call any extract scripts.
Produces:
  - missingness summary per source
  - cross-source check: % of OBF OCR ingredients with zero match in CosIng
"""
import sys
import pandas as pd
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import PROCESSED_DIR, RAW_IMAGES, get_logger

log = get_logger("eda_report")


def missingness_summary() -> pd.DataFrame:
    rows = []

    text_path = PROCESSED_DIR / "pubmed_processed.parquet"
    if text_path.exists():
        df = pd.read_parquet(text_path)
        rows.append({"source": "pubmed_text", "n_rows": len(df),
                     "pct_missing_key_field": 0.0})

    img_path    = PROCESSED_DIR / "images_processed.parquet"
    manifest_path = RAW_IMAGES / "manifest.csv"
    if img_path.exists() and manifest_path.exists():
        processed = pd.read_parquet(img_path)
        manifest  = pd.read_csv(manifest_path)
        pct_broken = 100 * (1 - len(processed) / max(len(manifest), 1))
        rows.append({"source": "obf_images", "n_rows": len(processed),
                     "pct_missing_key_field": round(pct_broken, 1)})

    audio_path = PROCESSED_DIR / "audio_processed.parquet"
    if audio_path.exists():
        df = pd.read_parquet(audio_path)
        pct_whisper = 100 * df["used_whisper"].mean() if "used_whisper" in df else 0.0
        rows.append({"source": "audio_video", "n_rows": len(df),
                     "pct_missing_key_field": round(pct_whisper, 1)})

    cosing_path = PROCESSED_DIR / "cosing_processed.parquet"
    if cosing_path.exists():
        df = pd.read_parquet(cosing_path)
        pct_unknown = 100 * (df["regulatory_status"] == "unknown").mean() \
                      if "regulatory_status" in df else 0.0
        rows.append({"source": "cosing", "n_rows": len(df),
                     "pct_missing_key_field": round(pct_unknown, 1)})

    fda_path = PROCESSED_DIR / "openfda_processed.parquet"
    if fda_path.exists():
        df = pd.read_parquet(fda_path, columns=["safetyreportid"])
        rows.append({"source": "openfda", "n_rows": len(df),
                     "pct_missing_key_field": 0.0})

    return pd.DataFrame(rows)


def cross_source_match_check() -> dict:
    cosing_path = PROCESSED_DIR / "cosing_processed.parquet"
    img_path    = PROCESSED_DIR / "images_processed.parquet"
    if not (cosing_path.exists() and img_path.exists()):
        return {}

    cosing = pd.read_parquet(cosing_path)
    images = pd.read_parquet(img_path)

    cosing_names = set(cosing["inci_name_std"].dropna())
    matched, total = 0, 0
    for ocr_text in images["ocr_text"].dropna():
        tokens = [t.strip().lower()
                  for t in ocr_text.replace("\n", ",").split(",") if t.strip()]
        for t in tokens:
            total += 1
            if t in cosing_names:
                matched += 1

    if total == 0:
        return {}
    return {
        "obf_ingredients_checked": total,
        "matched_in_cosing":       matched,
        "pct_unmatched":           round(100 * (1 - matched / total), 1),
    }


def main():
    summary = missingness_summary()
    out_path = PROCESSED_DIR / "eda_missingness_summary.csv"
    summary.to_csv(out_path, index=False)
    log.info("Missingness summary:\n%s", summary.to_string(index=False))
    log.info("Saved -> %s", out_path)

    cross = cross_source_match_check()
    if cross:
        log.info("Cross-source check (OBF vs CosIng): %s", cross)
        pd.DataFrame([cross]).to_csv(
            PROCESSED_DIR / "eda_cross_source_check.csv", index=False
        )
    else:
        log.warning("Skipped cross-source check — need both parquet files.")

    log.info("Done.")


if __name__ == "__main__":
    main()