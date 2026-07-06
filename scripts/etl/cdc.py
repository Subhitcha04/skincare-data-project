"""
CDC (Change Data Capture) -- introductory scope.

openFDA adverse event reports occasionally get amended after initial
submission. This implements timestamp-based polling: compare each incoming
record's last-modified-equivalent field against what's already stored, and
classify as insert / update / no-op.

Production-grade alternative (not implemented here, just noted for the
write-up): log-based CDC via Postgres WAL + Debezium, which streams row-level
changes directly off the database transaction log instead of polling.
"""
import sys
import json
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_STRUCTURED, POSTGRES_URL, get_logger

log = get_logger("cdc")


def classify_records(engine, new_records: list[dict]) -> dict:
    """Return counts of insert/update/no-op for a batch of openFDA records."""
    counts = {"insert": 0, "update": 0, "noop": 0}

    with engine.begin() as conn:
        for rec in new_records:
            report_id = rec.get("safetyreportid")
            incoming_modified = rec.get("receiptdate") or rec.get("receivedate")
            if not report_id:
                continue

            existing = conn.execute(
                text("SELECT receivedate FROM adverse_events WHERE safetyreportid = :id"),
                {"id": report_id},
            ).fetchone()

            if existing is None:
                counts["insert"] += 1
                conn.execute(
                    text("""
                        INSERT INTO adverse_events (safetyreportid, ingredient_term, receivedate, raw_payload)
                        VALUES (:id, :term, :date, :payload)
                        ON CONFLICT (safetyreportid) DO NOTHING
                    """),
                    {
                        "id": report_id,
                        "term": rec.get("ingredient_term", ""),
                        "date": pd.to_datetime(incoming_modified, format="%Y%m%d", errors="coerce"),
                        "payload": json.dumps(rec, default=str),
                    },
                )
            else:
                stored_date = str(existing[0])
                if str(incoming_modified) != stored_date:
                    counts["update"] += 1
                    conn.execute(
                        text("""
                            UPDATE adverse_events
                            SET receivedate = :date, raw_payload = :payload, loaded_at = NOW()
                            WHERE safetyreportid = :id
                        """),
                        {
                            "id": report_id,
                            "date": pd.to_datetime(incoming_modified, format="%Y%m%d", errors="coerce"),
                            "payload": json.dumps(rec, default=str),
                        },
                    )
                else:
                    counts["noop"] += 1

    return counts


def main():
    if not POSTGRES_URL:
        log.error("POSTGRES_URL not set -- cannot run CDC classification.")
        return

    engine = create_engine(POSTGRES_URL)
    total_counts = {"insert": 0, "update": 0, "noop": 0}

    for path in RAW_STRUCTURED.glob("openfda_*.json"):
        records = json.loads(path.read_text())
        counts = classify_records(engine, records)
        for k in total_counts:
            total_counts[k] += counts[k]

    log.info("CDC classification totals: %s", total_counts)


if __name__ == "__main__":
    main()
