"""
Load: the "L" step.

- CosIng        -> full load (truncate + reload) into the relational staging table
- PubMed,
  openFDA,
  YouTube       -> incremental load, driven by the etl_watermarks table
- OBF raw JSON  -> loaded as-is into MongoDB (NoSQL source, schema varies per product)

Run sql/staging_schema.sql against your Postgres DB once before the first run.
"""
import sys
import json
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from pymongo import MongoClient

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import (
    PROCESSED_DIR, RAW_STRUCTURED, POSTGRES_URL, MONGO_URI, MONGO_DB, get_logger
)

log = get_logger("load")


def get_engine():
    if not POSTGRES_URL:
        raise RuntimeError("POSTGRES_URL not set in .env")
    return create_engine(POSTGRES_URL)


def full_load_cosing(engine):
    path = PROCESSED_DIR / "cosing_processed.parquet"
    if not path.exists():
        log.warning("cosing_processed.parquet missing -- skipping CosIng load.")
        return
    df = pd.read_parquet(path)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE cosing_staging"))
    df.to_sql("cosing_staging", engine, if_exists="append", index=False)
    log.info("Full-loaded %d CosIng rows", len(df))


def get_watermark(engine, source: str) -> str | None:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT last_value FROM etl_watermarks WHERE source_name = :s"),
            {"s": source},
        ).fetchone()
    return row[0] if row else None


def set_watermark(engine, source: str, value: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO etl_watermarks (source_name, last_value, updated_at)
                VALUES (:s, :v, NOW())
                ON CONFLICT (source_name) DO UPDATE
                SET last_value = :v, updated_at = NOW()
            """),
            {"s": source, "v": value},
        )


def incremental_load_pubmed(engine):
    path = PROCESSED_DIR / "pubmed_processed.parquet"
    if not path.exists():
        log.warning("pubmed_processed.parquet missing -- skipping PubMed load.")
        return
    df = pd.read_parquet(path)[["pmid", "ingredient_term", "abstract_length"]].dropna(subset=["pmid"])
    df["pmid"] = df["pmid"].astype(str)

    watermark = get_watermark(engine, "pubmed")
    if watermark:
        df = df[df["pmid"].astype(int) > int(watermark)]

    if df.empty:
        log.info("PubMed: no new records since watermark %s", watermark)
        return

    # Use ON CONFLICT DO NOTHING to make re-runs safe
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy import Table, MetaData
    meta = MetaData()
    meta.reflect(bind=engine, only=["pubmed_evidence"])
    tbl = meta.tables["pubmed_evidence"]

    with engine.begin() as conn:
        for chunk_start in range(0, len(df), 500):
            chunk = df.iloc[chunk_start:chunk_start+500].to_dict(orient="records")
            stmt = pg_insert(tbl).values(chunk).on_conflict_do_nothing(index_elements=["pmid"])
            conn.execute(stmt)

    set_watermark(engine, "pubmed", str(df["pmid"].astype(int).max()))
    log.info("Incrementally loaded %d new PubMed rows", len(df))


def incremental_load_openfda(engine):
    path = PROCESSED_DIR / "openfda_processed.parquet"
    if not path.exists():
        log.warning("openfda_processed.parquet missing -- skipping openFDA load.")
        return
    df = pd.read_parquet(path)
    if "safetyreportid" not in df.columns:
        log.warning("openFDA frame missing safetyreportid -- skipping.")
        return

    watermark = get_watermark(engine, "openfda")
    if watermark:
        df = df[df["receivedate"].astype(str) > watermark]

    if df.empty:
        log.info("openFDA: no new records since watermark %s", watermark)
        return

    out = pd.DataFrame({
        "safetyreportid": df["safetyreportid"],
        "ingredient_term": df.get("ingredient_term", ""),
        "receivedate": pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce"),
        "raw_payload": df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1),
    })
    # Replace NaN in raw_payload with null so PostgreSQL accepts it as valid JSON
    out["raw_payload"] = out["raw_payload"].str.replace(r': NaN', ': null', regex=True)
    out.to_sql("adverse_events", engine, if_exists="append", index=False,
               method="multi", chunksize=200)
    set_watermark(engine, "openfda", str(df["receivedate"].astype(str).max()))
    log.info("Incrementally loaded %d new openFDA rows", len(out))


def load_obf_raw_to_mongo():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db["openbeautyfacts_raw"]

    n_loaded = 0
    for path in RAW_STRUCTURED.glob("obf_*_page*.json"):
        data = json.loads(path.read_text())
        products = data.get("products", [])
        for p in products:
            # use the OBF product code as _id so re-runs upsert instead of duplicating
            p["_id"] = p.get("code", str(hash(json.dumps(p, default=str))))
            collection.replace_one({"_id": p["_id"]}, p, upsert=True)
            n_loaded += 1

    log.info("Upserted %d OBF product docs into MongoDB (%s.%s)", n_loaded, MONGO_DB, "openbeautyfacts_raw")


def main():
    engine = get_engine()
    full_load_cosing(engine)
    incremental_load_pubmed(engine)
    incremental_load_openfda(engine)
    load_obf_raw_to_mongo()


if __name__ == "__main__":
    main()
