-- Run against your Postgres (or adapt slightly for MySQL) staging DB before
-- running scripts/etl/load.py.

CREATE TABLE IF NOT EXISTS cosing_staging (
    inci_name           TEXT PRIMARY KEY,
    inci_name_std       TEXT,
    function            TEXT,
    regulatory_status   TEXT,
    concentration_limit TEXT,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_code     TEXT PRIMARY KEY,
    category          TEXT,
    ingredients_text  TEXT,
    risk_score        NUMERIC,
    loaded_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pubmed_evidence (
    pmid              TEXT PRIMARY KEY,
    ingredient_term   TEXT,
    abstract_length   INTEGER,
    loaded_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS adverse_events (
    safetyreportid    TEXT PRIMARY KEY,
    ingredient_term   TEXT,
    receivedate       DATE,
    raw_payload       JSONB,
    loaded_at         TIMESTAMP DEFAULT NOW()
);

-- Watermark table drives incremental loads for PubMed / openFDA / YouTube.
CREATE TABLE IF NOT EXISTS etl_watermarks (
    source_name       TEXT PRIMARY KEY,
    last_value        TEXT,        -- last processed date or ID, source-dependent
    updated_at        TIMESTAMP DEFAULT NOW()
);
