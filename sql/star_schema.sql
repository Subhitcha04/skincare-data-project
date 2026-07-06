-- ============================================================
-- GlowCheck Star Schema — Dimensional Model (OLAP layer)
-- Grain: one ingredient-safety observation per product per date
-- Run after staging_schema.sql
-- ============================================================

-- ─── Dimension: Date ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        SERIAL PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            INTEGER,
    month           INTEGER,
    month_name      TEXT,
    quarter         INTEGER,
    day_of_week     TEXT
);

-- ─── Dimension: Ingredient ───────────────────────────────────
-- Sourced from CosIng; one row per standardized INCI name
CREATE TABLE IF NOT EXISTS dim_ingredient (
    ingredient_key      SERIAL PRIMARY KEY,
    inci_name           TEXT NOT NULL UNIQUE,
    inci_name_std       TEXT,
    function            TEXT,
    regulatory_status   TEXT,           -- 'banned' | 'restricted' | 'allowed' | 'unknown'
    concentration_limit TEXT,
    risk_weight         NUMERIC(4,2)    -- derived: banned=1.0, restricted=0.8, allowed=0.1, unknown=0.4
);

-- ─── Dimension: Product ──────────────────────────────────────
-- Sourced from Open Beauty Facts
CREATE TABLE IF NOT EXISTS dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_code    TEXT NOT NULL UNIQUE,
    product_name    TEXT,
    category        TEXT,
    brand           TEXT,
    country         TEXT
);

-- ─── Dimension: Source ───────────────────────────────────────
-- Which pipeline/data source produced this record
CREATE TABLE IF NOT EXISTS dim_source (
    source_key      SERIAL PRIMARY KEY,
    source_name     TEXT NOT NULL UNIQUE,   -- 'openFDA' | 'PubMed' | 'OBF' | 'CosIng' | 'YouTube'
    source_type     TEXT,                   -- 'REST_API' | 'flat_file' | 'NoSQL' | 'RSS'
    extraction_method TEXT
);

-- ─── Dimension: Observer/Reporter ────────────────────────────
-- Country/reporter from adverse event records
CREATE TABLE IF NOT EXISTS dim_reporter (
    reporter_key        SERIAL PRIMARY KEY,
    reporter_country    TEXT NOT NULL UNIQUE,
    region              TEXT
);

-- ─── Fact: Safety Event ──────────────────────────────────────
-- Grain: one adverse event report mentioning one ingredient
CREATE TABLE IF NOT EXISTS fact_safety_event (
    event_key           SERIAL PRIMARY KEY,
    date_key            INTEGER REFERENCES dim_date(date_key),
    ingredient_key      INTEGER REFERENCES dim_ingredient(ingredient_key),
    source_key          INTEGER REFERENCES dim_source(source_key),
    reporter_key        INTEGER REFERENCES dim_reporter(reporter_key),

    -- Measures
    safetyreportid      TEXT,
    is_serious          BOOLEAN,
    event_count         INTEGER DEFAULT 1,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- ─── Fact: Ingredient Evidence ───────────────────────────────
-- Grain: one PubMed abstract per ingredient term per date
CREATE TABLE IF NOT EXISTS fact_ingredient_evidence (
    evidence_key        SERIAL PRIMARY KEY,
    date_key            INTEGER REFERENCES dim_date(date_key),
    ingredient_key      INTEGER REFERENCES dim_ingredient(ingredient_key),
    source_key          INTEGER REFERENCES dim_source(source_key),

    -- Measures
    pmid                TEXT,
    abstract_length     INTEGER,
    evidence_score      NUMERIC(5,4),   -- normalized PubMed hit count 0..1
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- ─── Fact: Product Risk ──────────────────────────────────────
-- Grain: one product risk assessment per product per date
CREATE TABLE IF NOT EXISTS fact_product_risk (
    risk_key            SERIAL PRIMARY KEY,
    date_key            INTEGER REFERENCES dim_date(date_key),
    product_key         INTEGER REFERENCES dim_product(product_key),
    source_key          INTEGER REFERENCES dim_source(source_key),

    -- Measures
    risk_score          NUMERIC(5,4),   -- avg risk weight across product ingredients
    flagged_ingredients INTEGER,        -- count of restricted/banned ingredients
    total_ingredients   INTEGER,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- ─── Populate dim_date (2010–2030) ───────────────────────────
INSERT INTO dim_date (full_date, year, month, month_name, quarter, day_of_week)
SELECT
    d::DATE,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::INTEGER,
    TO_CHAR(d, 'Day')
FROM generate_series('2010-01-01'::DATE, '2030-12-31'::DATE, '1 day') d
ON CONFLICT (full_date) DO NOTHING;

-- ─── Populate dim_source ─────────────────────────────────────
INSERT INTO dim_source (source_name, source_type, extraction_method) VALUES
    ('openFDA',  'REST_API',   'Paginated GET with exponential backoff + date-range chunking'),
    ('PubMed',   'REST_API',   'NCBI E-utilities esearch/efetch with watermark'),
    ('OBF',      'REST_API',   'Open Beauty Facts category pagination + image download'),
    ('CosIng',   'flat_file',  'Bulk CSV export, truncate-and-reload'),
    ('YouTube',  'REST_API',   'YouTube Data API v3 search + captions'),
    ('Podcast',  'RSS',        'RSS feed parse + MP3 manifest download')
ON CONFLICT (source_name) DO NOTHING;

-- ─── Indexes for common analytical queries ───────────────────
CREATE INDEX IF NOT EXISTS idx_safety_date     ON fact_safety_event(date_key);
CREATE INDEX IF NOT EXISTS idx_safety_ing      ON fact_safety_event(ingredient_key);
CREATE INDEX IF NOT EXISTS idx_evidence_ing    ON fact_ingredient_evidence(ingredient_key);
CREATE INDEX IF NOT EXISTS idx_risk_product    ON fact_product_risk(product_key);
CREATE INDEX IF NOT EXISTS idx_risk_date       ON fact_product_risk(date_key);

-- ─── Sample OLAP queries ──────────────────────────────────────
-- Q1: Adverse events by ingredient by year
-- SELECT i.inci_name_std, d.year, COUNT(*) AS events
-- FROM fact_safety_event f
-- JOIN dim_ingredient i ON f.ingredient_key = i.ingredient_key
-- JOIN dim_date d       ON f.date_key       = d.date_key
-- GROUP BY i.inci_name_std, d.year
-- ORDER BY events DESC;

-- Q2: High-risk products with restricted ingredients
-- SELECT p.product_name, r.risk_score, r.flagged_ingredients
-- FROM fact_product_risk r
-- JOIN dim_product p ON r.product_key = p.product_key
-- WHERE r.risk_score > 0.7
-- ORDER BY r.risk_score DESC;

-- Q3: Evidence vs adverse events per ingredient (cross-fact join)
-- SELECT i.inci_name_std,
--        SUM(e.event_count)    AS total_adverse_events,
--        AVG(ev.evidence_score) AS avg_evidence_score
-- FROM dim_ingredient i
-- LEFT JOIN fact_safety_event      e  ON i.ingredient_key = e.ingredient_key
-- LEFT JOIN fact_ingredient_evidence ev ON i.ingredient_key = ev.ingredient_key
-- GROUP BY i.inci_name_std
-- ORDER BY total_adverse_events DESC;