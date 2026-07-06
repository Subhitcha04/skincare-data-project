"""
Weeks 7-8 — Rollback Simulation Test

Proves that atomic transactions work correctly:
1. Records rows BEFORE the test load
2. Runs atomic_load with simulate_failure=True (fails mid-insert)
3. Records rows AFTER — count must be identical (no partial data)
"""
import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import POSTGRES_URL, PROCESSED_DIR, get_logger
from scripts.production.resilient_pipeline import atomic_load

log = get_logger("test_rollback")


def row_count(engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def main():
    engine = create_engine(POSTGRES_URL)

    pubmed_path = PROCESSED_DIR / "pubmed_processed.parquet"
    if not pubmed_path.exists():
        log.error("pubmed_processed.parquet not found — run preprocessing first.")
        return

    df = pd.read_parquet(pubmed_path)[
        ["pmid", "ingredient_term", "abstract_length"]
    ].dropna(subset=["pmid"])
    df["pmid"] = df["pmid"].astype(str)
    df["abstract_length"] = pd.to_numeric(
        df["abstract_length"], errors="coerce"
    ).fillna(0).astype(int)

    # Snapshot row count before
    before = row_count(engine, "pubmed_evidence")
    log.info("Row count BEFORE simulated failure: %d", before)

    # Run with failure simulation
    log.info("Running atomic_load with simulate_failure=True ...")
    success = atomic_load(
        engine, df, "pubmed_evidence", "pmid",
        simulate_failure=True
    )

    # Snapshot row count after
    after = row_count(engine, "pubmed_evidence")
    log.info("Row count AFTER  simulated failure: %d", after)

    # Assert atomicity
    if before == after:
        log.info("ATOMICITY CONFIRMED — row count unchanged (%d = %d)", before, after)
        log.info("Partial inserts were fully rolled back.")
    else:
        log.error(
            "ATOMICITY VIOLATED — row count changed from %d to %d",
            before, after
        )


if __name__ == "__main__":
    main()