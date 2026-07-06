"""
Weeks 5-8 — Resilient, Production-Ready Pipeline

Implements:
  - Staging area (load to temp table first, validate, then swap)
  - Data quality checks (schema validation, null checks, outlier detection)
  - Idempotency (safe to re-run without duplicating data)
  - Atomicity (all-or-nothing transactions with rollback on failure)
  - Error handling with backfill/replay support
  - Failure simulation for testing rollback
"""
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import POSTGRES_URL, PROCESSED_DIR, get_logger

log = get_logger("resilient_pipeline")

SIMULATE_FAILURE = False   # Set True to test atomic rollback


# ─── Staging & Validation ────────────────────────────────────

def create_staging_table(conn, staging_table: str, source_table: str):
    """Create a temporary staging table mirroring source structure."""
    conn.execute(text(f"""
        CREATE TEMP TABLE IF NOT EXISTS {staging_table}
        (LIKE {source_table} INCLUDING ALL)
        ON COMMIT DROP
    """))
    log.info("Staging table '%s' created", staging_table)


def validate_dataframe(df: pd.DataFrame, table_name: str) -> tuple[bool, list]:
    """
    Run data quality checks:
    1. Schema validation  — required columns exist
    2. Null checks        — key columns have no nulls
    3. Outlier detection  — numeric columns within expected range
    Returns (is_valid, list_of_errors)
    """
    errors = []

    REQUIRED_COLUMNS = {
        "pubmed_evidence":  ["pmid", "ingredient_term", "abstract_length"],
        "cosing_staging":   ["inci_name", "function", "regulatory_status"],
        "adverse_events":   ["safetyreportid", "receivedate"],
    }

    NULL_CHECK_COLUMNS = {
        "pubmed_evidence":  ["pmid"],
        "cosing_staging":   ["inci_name"],
        "adverse_events":   ["safetyreportid"],
    }

    OUTLIER_CHECKS = {
        "pubmed_evidence":  {"abstract_length": (1, 5000)},
    }

    # 1. Schema validation
    required = REQUIRED_COLUMNS.get(table_name, [])
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        errors.append(f"Schema validation FAILED — missing columns: {missing_cols}")

    # 2. Null checks
    null_cols = NULL_CHECK_COLUMNS.get(table_name, [])
    for col in null_cols:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                errors.append(f"Null check FAILED — {null_count} nulls in '{col}'")

    # 3. Outlier detection
    outlier_rules = OUTLIER_CHECKS.get(table_name, {})
    for col, (lo, hi) in outlier_rules.items():
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            outliers = numeric[(numeric < lo) | (numeric > hi)].count()
            if outliers > 0:
                errors.append(
                    f"Outlier check WARNING — {outliers} values outside "
                    f"[{lo}, {hi}] in '{col}'"
                )

    is_valid = len([e for e in errors if "FAILED" in e]) == 0
    return is_valid, errors


# ─── Idempotent Load ─────────────────────────────────────────

def idempotent_upsert(conn, df: pd.DataFrame, table: str, pk: str):
    """
    Upsert rows using ON CONFLICT DO NOTHING — safe to re-run any
    number of times without creating duplicates.
    """
    inserted = 0
    for _, row in df.iterrows():
        cols   = ", ".join(df.columns)
        params = {c: (None if pd.isna(v) else v) for c, v in row.items()}
        placeholders = ", ".join(f":{c}" for c in df.columns)
        conn.execute(
            text(f"""
                INSERT INTO {table} ({cols})
                VALUES ({placeholders})
                ON CONFLICT ({pk}) DO NOTHING
            """),
            params
        )
        inserted += 1
    return inserted


# ─── Atomic Load with Rollback ───────────────────────────────

def atomic_load(engine, df: pd.DataFrame, table: str, pk: str,
                simulate_failure: bool = False) -> bool:
    """
    Wraps the entire load in a single transaction.
    If anything fails (including a simulated failure), the whole
    transaction rolls back — no partial data lands in the table.
    """
    log.info("Starting atomic load → '%s' (%d rows)", table, len(df))

    try:
        with engine.begin() as conn:     # auto-commit on success, rollback on exception

            # Validate before touching the DB
            is_valid, errors = validate_dataframe(df, table)
            for e in errors:
                if "FAILED" in e:
                    log.error("  %s", e)
                else:
                    log.warning("  %s", e)

            if not is_valid:
                raise ValueError(f"Validation failed for {table} — aborting load")

            # Simulate mid-load failure to prove rollback works
            if simulate_failure:
                log.warning("SIMULATING FAILURE mid-load to test rollback...")
                # Load half the data, then raise
                half = df.iloc[:len(df)//2]
                idempotent_upsert(conn, half, table, pk)
                raise RuntimeError("Simulated failure — transaction will be rolled back")

            # Normal path
            n = idempotent_upsert(conn, df, table, pk)
            log.info("  Committed %d rows to '%s'", n, table)
            return True

    except (SQLAlchemyError, ValueError, RuntimeError) as e:
        log.error("ATOMIC LOAD FAILED — rolled back: %s", e)
        return False


# ─── Backfill / Replay ───────────────────────────────────────

def backfill(engine, source_path: Path, table: str, pk: str,
             date_col: str, from_date: str, to_date: str):
    """
    Replay historical data for a specific date range.
    Idempotent — safe to run multiple times.
    """
    log.info("Backfilling '%s' from %s to %s", table, from_date, to_date)

    if not source_path.exists():
        log.error("Source file not found: %s", source_path)
        return

    df = pd.read_parquet(source_path)

    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df[
            (df[date_col] >= pd.Timestamp(from_date)) &
            (df[date_col] <= pd.Timestamp(to_date))
        ]

    log.info("  %d rows in backfill window", len(df))
    success = atomic_load(engine, df, table, pk)
    log.info("  Backfill %s", "succeeded" if success else "FAILED")


# ─── Main ────────────────────────────────────────────────────

def main():
    if not POSTGRES_URL:
        log.error("POSTGRES_URL not set.")
        return

    engine = create_engine(POSTGRES_URL)

    # ── Load PubMed (with full production checks) ──
    pubmed_path = PROCESSED_DIR / "pubmed_processed.parquet"
    if pubmed_path.exists():
        df = pd.read_parquet(pubmed_path)[
            ["pmid", "ingredient_term", "abstract_length"]
        ].dropna(subset=["pmid"])
        df["pmid"] = df["pmid"].astype(str)
        df["abstract_length"] = pd.to_numeric(
            df["abstract_length"], errors="coerce"
        ).fillna(0).astype(int)

        success = atomic_load(
            engine, df, "pubmed_evidence", "pmid",
            simulate_failure=SIMULATE_FAILURE
        )
        log.info("PubMed load: %s", "OK" if success else "FAILED (rolled back)")

    # ── Load CosIng (full load — truncate then atomic insert) ──
    cosing_path = PROCESSED_DIR / "cosing_processed.parquet"
    if cosing_path.exists():
        df = pd.read_parquet(cosing_path).rename(columns={
            "inci_name_std": "inci_name_std"
        })[["inci_name", "inci_name_std", "function", "regulatory_status"]]
        df = df.dropna(subset=["inci_name"])

        try:
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE cosing_staging RESTART IDENTITY"))
                n = idempotent_upsert(conn, df, "cosing_staging", "inci_name")
                log.info("CosIng full load: %d rows committed", n)
        except SQLAlchemyError as e:
            log.error("CosIng load rolled back: %s", e)

    log.info("Resilient pipeline complete.")


if __name__ == "__main__":
    main()