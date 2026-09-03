# GlowCheck — Git Branching Strategy

## Branch Structure

```
main
  └── develop
        ├── feature/extract-pubmed
        ├── feature/extract-openfda
        ├── feature/etl-transform
        ├── feature/star-schema
        ├── feature/airflow-dag
        ├── fix/openfda-chunking
        └── release/v1.0
```

## Branch Rules

| Branch | Purpose | Who merges | CI Required |
|--------|---------|-----------|-------------|
| `main` | Production — always deployable | PR from release only | Yes — all jobs green |
| `develop` | Integration — working code | PR from feature/fix | Yes — tests pass |
| `feature/*` | New pipeline feature | Developer | Lint + unit tests |
| `fix/*` | Bug fixes | Developer | Unit tests |
| `release/*` | Release prep | Team lead | Full CI suite |

## Workflow

1. Branch from `develop`:
   `git checkout -b feature/your-feature develop`

2. Commit with conventional messages:
   - `feat: add openFDA date-range chunking`
   - `fix: resolve %2B encoding in Lucene query`
   - `test: add CDC classification unit tests`
   - `docs: update README with Airflow setup`
   - `refactor: extract watermark logic to helper`

3. Push and open PR to `develop` — CI runs automatically

4. After review + CI green → merge to `develop`

5. When ready to release → merge `develop` to `main`

## Pipeline as Code

All infrastructure is code in this repo:
- `sql/staging_schema.sql`     — OLTP schema
- `sql/star_schema.sql`        — OLAP star schema
- `dags/glowcheck_dag.py`      — Airflow DAG definition
- `kafka/kafka_producer.py`    — Kafka producer
- `kafka/kafka_consumer.py`    — Kafka consumer
- `.github/workflows/ci.yml`   — CI/CD pipeline
- `requirements.txt`           — dependency pinning
- `.env.example`               — config template (never commit .env)