# GlowCheck — Skincare Ingredient Safety & ETL Platform

An end-to-end multi-modal data engineering project that collects, preprocesses, and loads skincare ingredient safety data from five heterogeneous sources into a unified analytical warehouse.

---

## Problem Statement

Skincare product claims are largely unverified — brands use marketing language unsupported by evidence, and consumers have no easy way to cross-reference a product's ingredient list against regulatory safety records and peer-reviewed research. GlowCheck builds the data backbone for a platform that does exactly that: ingest ingredient data from multiple sources, cross-reference against global regulatory databases, surface adverse event patterns, and score products by evidence-backed safety risk.

---

## Architecture

```
REST APIs / RSS / CSV
        │
   ┌────▼────┐
   │ Extract │  PubMed · OpenBeautyFacts · openFDA · YouTube · Podcasts · CosIng
   └────┬────┘
        │
   ┌────▼──────┐
   │ Transform │  Clean · Standardize · Join · Aggregate · Feature Engineer
   └────┬──────┘
        │
   ┌────▼────────────────────────┐
   │         Load                │
   │  PostgreSQL (structured)    │  Full load / Incremental / CDC
   │  MongoDB    (documents)     │  Upsert by barcode
   └─────────────────────────────┘
        │
   ┌────▼──────────────┐
   │  Star Schema OLAP │  Fact + Dimension tables for analytical queries
   └───────────────────┘
```

---

## Data Sources

| Type | Source | Extraction Method |
|---|---|---|
| Text | PubMed (NCBI E-utilities) | REST API + pagination |
| Image | Open Beauty Facts | REST API + manifest download |
| Audio | AAD Dermatology Podcast | RSS flat-file + MP3 download |
| Video | YouTube Data API | REST API + captions |
| Medical | openFDA adverse events | REST API + date-range chunking |
| Structured | EU CosIng registry | Bulk CSV flat-file |

---

## Project Structure

```
skincare-data-project/
├── config.py                        # Central config, env vars, paths
├── requirements.txt
├── .env.example                     # Copy to .env and fill credentials
├── sql/
│   ├── staging_schema.sql           # OLTP staging tables
│   └── star_schema.sql              # OLAP star schema (Week 3)
├── scripts/
│   ├── extract/                     # One script per data source
│   │   ├── extract_pubmed.py
│   │   ├── extract_openbeautyfacts.py
│   │   ├── extract_openfda.py       # Incremental + date-chunked pagination
│   │   ├── extract_podcasts.py
│   │   ├── extract_youtube.py
│   │   └── extract_cosing.py
│   ├── preprocess/                  # One script per data type
│   │   ├── preprocess_text.py
│   │   ├── preprocess_images.py
│   │   ├── preprocess_audio.py
│   │   └── preprocess_structured.py
│   ├── eda/
│   │   └── eda_report.py            # Missingness + cross-source validation
│   ├── etl/
│   │   ├── transform.py             # Join, aggregate, score
│   │   ├── load.py                  # Full + incremental load to PostgreSQL/MongoDB
│   │   └── cdc.py                   # Change Data Capture (insert/update/no-op)
│   ├── batch/
│   │   └── batch_pipeline.py        # Week 4: end-to-end batch ETL
│   └── production/
│       ├── resilient_pipeline.py    # Weeks 5-8: staging, validation, atomicity
│       └── test_rollback.py         # Simulate failure → prove rollback works
├── raw/                             # Raw extracted files (gitignored)
├── staging/                         # Intermediate staging parquet files
└── processed/                       # Feature-engineered parquet files (gitignored)
```

---

## Setup

```bash
# 1. Clone and create virtual environment
git clone https://github.com/Subhitcha04/skincare-data-project.git
cd skincare-data-project
python -m venv venv
venv\Scripts\activate      # Windows
# source venv/bin/activate  # Mac/Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Fill in POSTGRES_URL, MONGO_URI, API keys

# 4. Create database
psql -U postgres -c "CREATE DATABASE skincare_staging;"
psql -U postgres -d skincare_staging -f sql/staging_schema.sql
psql -U postgres -d skincare_staging -f sql/star_schema.sql
```

---

## Running the Pipeline

```bash
# Week 1-2: Extract all sources
python scripts/extract/extract_pubmed.py
python scripts/extract/extract_openbeautyfacts.py
python scripts/extract/extract_openfda.py
python scripts/extract/extract_cosing.py
python scripts/extract/extract_podcasts.py
python scripts/extract/extract_youtube.py

# Week 1-2: Preprocess + EDA
python scripts/preprocess/preprocess_text.py
python scripts/preprocess/preprocess_images.py
python scripts/preprocess/preprocess_audio.py
python scripts/preprocess/preprocess_structured.py
python scripts/eda/eda_report.py

# Week 2: ETL
python scripts/etl/transform.py
python scripts/etl/load.py
python scripts/etl/cdc.py

# Week 4: Batch pipeline
python scripts/batch/batch_pipeline.py

# Weeks 5-8: Production pipeline
python scripts/production/resilient_pipeline.py

# Test atomic rollback
python scripts/production/test_rollback.py
```

---

## Key Engineering Decisions

**Why two loading strategies?**
CosIng is small and stable → full load (truncate + reload). openFDA and PubMed grow continuously → incremental load with watermarks so reruns don't duplicate records.

**Why CDC on openFDA?**
Adverse event reports get amended after submission. Timestamp-based CDC classifies each incoming record as insert, update, or no-op, keeping the warehouse accurate without re-downloading the entire dataset.

**Why MongoDB for Open Beauty Facts?**
Each product has a different, irregular set of fields. Forcing this into a rigid relational schema would require nullable columns for most fields. MongoDB's flexible document model is the right tool here.

**Why chunked pagination for openFDA?**
openFDA's API hard-caps skip-based pagination at 25,000 records. High-volume terms like "vitamin c" exceed this. Date-range chunking (90-day windows) keeps each window under the ceiling, avoiding 500 errors and timeouts.

---

## Results

## Dashboard

![GlowCheck Power BI Dashboard](assets/powerbi_dashboard_screenshot.png)
| Table | Rows | Load Type |
|---|---|---|
| adverse_events | 53,462 | Incremental + CDC |
| cosing_staging | 30,079 | Full load |
| pubmed_evidence | 3,055 | Incremental |
| MongoDB (OBF) | 203 docs | Upsert |

CDC classified: **9,891 inserts**, **44,166 updates**, **0 no-ops**

---

## Tech Stack

Python · PostgreSQL · MongoDB · Pandas · SQLAlchemy · scikit-learn · librosa · Tesseract OCR · Whisper · REST APIs · RSS feeds · Power BI *(planned)*

---

## Author

Subhitcha S · CIT Coimbatore · M.Sc. Decision and Computing Sciences · [GitHub: Subhitcha04](https://github.com/Subhitcha04)