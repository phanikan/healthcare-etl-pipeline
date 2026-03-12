# Healthcare Claims ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.7-red?logo=apache-airflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![Azure](https://img.shields.io/badge/Azure-Data%20Lake-0078D4?logo=microsoft-azure)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## Why I built this

At MedCore Health Systems (a regional insurance client I worked with), we were processing healthcare claims through a combination of manual Excel exports, scheduled SQL scripts, and a lot of hoping things didn't break overnight.

The worst part wasn't the bad data — dirty data is everywhere in healthcare. The worst part was **not knowing when something had failed**. A pipeline would silently drop records, or an ICD-10 code would come in with a format we hadn't seen before, and we'd only find out two days later when someone noticed the claim count looked off in a Power BI report. By then the downstream damage was already done.

I built this pipeline to solve exactly that. The whole design is centered around observability — every stage logs what it did, quarantines what it couldn't handle (instead of silently dropping it), and writes a quality score on every run. If something breaks, you know immediately and you know why.

This is roughly the architecture I wish we'd had. I've simplified a few things (the real system had more source types and a messier schema), but the core logic — especially the quarantine pattern and the anomaly scoring — comes directly from problems I actually ran into.

---

## What it does

Takes raw healthcare insurance claims from CSV exports, cleans and validates them, and loads them into a PostgreSQL data warehouse. Runs on a daily schedule via Airflow. Also writes Parquet to Azure Data Lake for longer-term storage and BI tool access.

The main things it handles that I kept running into in real work:

- **Duplicate claims** — the same claim submitted twice from slightly different source systems. Happens more than you'd think.
- **Invalid ICD-10 codes** — usually a formatting thing (E119 instead of E11.9) but sometimes genuinely wrong. The pipeline tries to auto-correct the format issue and flags the rest.
- **Claim amounts that are statistically impossible** — outlier detection using IQR. Not dropped, just flagged with a score so analysts can review.
- **Silent failures** — everything that can't be fixed goes to a quarantine table with a reason code, not a log file nobody reads.

---

## Architecture

```
CSV / Excel exports  (from source systems)
        │
        ▼
  [ EXTRACT ]
  claims_extractor.py
  - reads files, normalizes column names
  - handles encoding issues (had a lot of latin-1 problems)
  - archives raw files by date
        │
        ▼
  [ TRANSFORM ]
  claims_transformer.py
  - deduplication on claim_id
  - ICD-10 format validation + auto-correction
  - date normalization (service_date, submission_date)
  - claim amount validation + IQR-based anomaly scoring
  - bad records go to quarantine table, not the trash
        │
        ▼
  [ LOAD ]
  warehouse_loader.py
  - upsert into PostgreSQL (fact_claims star schema)
  - Parquet to Azure Data Lake (date-partitioned)
  - pipeline_runs audit table on every execution
        │
        ▼
  [ AIRFLOW DAG ]
  healthcare_claims_dag.py
  - daily at 2am
  - data quality gate before transform runs
  - email alert if gate fails
  - 2 retries with 5-min delay
```

---

## Results on sample data (10,000 records)

```
Run Date:          2024-01-15
Records ingested:  10,200  (includes injected duplicates)
--------------------------------------------
Duplicates removed:     200
Invalid ICD-10 nulled:   51
Amount outliers:         98  → quarantine
Anomaly flags:          441  (score > 0.5, kept in pipeline)
--------------------------------------------
Clean records loaded:  9,902
Data quality score:    97.1%
Processing time:       ~22 seconds
```

These numbers are from running it on synthetic data I generated to mimic what the real source looked like — proportional amounts of duplicates, bad codes, and outliers based on what I actually saw.

---

## Tech stack

| What | Why I chose it |
|------|----------------|
| Python / Pandas | Did the heavy lifting for transform logic. Considered PySpark but overkill for this volume. |
| Apache Airflow | Needed proper scheduling with retry logic and visibility. Cron + scripts was what we had before and it was a mess to debug. |
| PostgreSQL | Star schema for the warehouse. Kept it simple — goal was analytical query speed, not OLTP. |
| Azure Data Lake | Parquet output for Power BI / long-term storage. Matches what I was working with at MedCore. |
| Docker Compose | One-command local setup. Spent too long in the past with "works on my machine" problems. |
| Great Expectations | Listed in requirements but only partially integrated — still figuring out the best way to use it in Airflow without it being overly verbose. |

---

## Project structure

```
healthcare-etl-pipeline/
├── dags/
│   └── healthcare_claims_dag.py      # Airflow DAG
├── src/
│   ├── extractors/
│   │   └── claims_extractor.py
│   ├── transformers/
│   │   └── claims_transformer.py
│   ├── loaders/
│   │   └── warehouse_loader.py
│   └── utils/
│       ├── logger.py
│       └── data_generator.py         # generates synthetic test data
├── tests/
│   └── test_transformer.py           # 17 unit tests
├── data/
│   └── raw/claims_sample.csv         # sample data (10k records)
├── docs/
│   └── schema_design.md
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Running it locally

**Prerequisites:** Docker, Python 3.10+

```bash
git clone https://github.com/kphani/healthcare-etl-pipeline
cd healthcare-etl-pipeline

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Generate sample data
python src/utils/data_generator.py --rows 10000

# Run the pipeline (dry run skips the DB load)
python src/run_pipeline.py --source data/raw/claims_sample.csv --dry-run

# Full Docker setup (Postgres + Airflow)
cp .env.example .env
docker-compose up -d
# Airflow UI at http://localhost:8080  (admin / admin)
```

**Tests:**
```bash
pytest tests/ -v
# 17 passing
```

---

## What I'd do differently / what's still missing

Honestly: the Airflow integration is more illustrative than production-ready. I've used Airflow in real work but this DAG would need more work to actually deploy — proper secrets management, connections setup, a metadata DB separate from the warehouse.

The anomaly scoring is also pretty basic. IQR works but in a real system I'd want it to be specialty-aware — a $50k orthopedic procedure looks like an outlier in a general model but isn't. That's something I want to improve.

Still on my list:
- [ ] dbt models on top of the warehouse
- [ ] Streamlit dashboard for pipeline run history and quality trends
- [ ] Proper Great Expectations checkpoints in the DAG
- [ ] Tests for extractor and loader (right now only transformer is covered)

---

## Schema

Star schema — details in [docs/schema_design.md](docs/schema_design.md)

```
fact_claims  (claim_id, patient_id, provider_npi, payer_name,
              service_date, icd10_code, cpt_code, claim_amount,
              approved_amount, denial_code, processing_status,
              anomaly_score, approval_rate, days_to_submission)
    │
    ├── dim_provider  (npi, name, specialty)
    ├── dim_payer     (payer_name, payer_type)
    └── dim_date      (date_key, year, quarter, month, week)
```

---

Phani Kumar — phanikumarda@gmail.com — [kphani.com](https://www.kphani.com)
