# healthcare-etl-pipeline

Python pipeline that pulls healthcare insurance claims from CSV exports, cleans them, and loads into PostgreSQL. Also writes parquet files to Azure Data Lake for the BI team.

---

## why I built this

At my last client (a healthcare insurance company, calling them MedCore here) we had claims data coming in through a mix of system exports and manual uploads. The processing "pipeline" was basically a few SQL scripts on a scheduler and someone checking the output every morning.

It mostly worked but the failure mode was bad. When something broke the pipeline would silently drop records or continue with bad data. You'd find out 2 days later when a dashboard number looked off. By then the claims had already gone out wrong.

I kept thinking about how to fix this properly and eventually just built it. The main thing I wanted was visibility - know what went in, what got rejected and why, what came out. So every run now produces a quality score and any record that can't be cleaned goes to a quarantine table with a reason, not just quietly dropped.

This repo is a cleaned-up version of that work. The real system had more source types and messier edge cases but the core approach is the same.

---

## what it does

- reads claims from CSV exports (handles a few different column formats from different source systems)
- removes duplicates on claim_id - had a double-submission problem at MedCore
- fixes ICD-10 codes - the export was stripping decimals so E11.9 came in as E119, had to detect and correct
- IQR-based anomaly scoring on claim amounts to catch obvious data entry errors
- upserts to postgres instead of truncate-reload so a mid-run failure doesn't wipe the table
- parquet export to Azure Data Lake, partitioned by date
- Airflow DAG for scheduling with a data quality gate before the transform step runs

---

## how to run

needs Python 3.10+ and Docker for the local Airflow/postgres setup

clone and install:

    git clone https://github.com/phanikan/healthcare-etl-pipeline
    cd healthcare-etl-pipeline
    python -m venv venv && source venv/bin/activate
    pip install -r requirements.txt

make some test data and run:

    python src/utils/data_generator.py --rows 10000
    python src/run_pipeline.py --source data/raw/claims_sample.csv --dry-run

(--dry-run skips the db write, just shows output)

full local setup with Airflow:

    cp .env.example .env
    # fill in db credentials in .env
    docker-compose up -d
    # Airflow at localhost:8080, admin/admin

tests:

    pytest tests/ -v
    # should be 17 passing

---

## output from a sample run

10k synthetic claims:

    records in:            10,200
    duplicates removed:       200
    ICD-10 corrections:        51
    amount outliers flagged:   98  -> quarantine
    clean records loaded:   9,902
    data quality score:     97.1%

---

## stack

- Python + Pandas for transforms
- Airflow for scheduling
- PostgreSQL, star schema
- Azure Data Lake for parquet storage
- Docker Compose for local dev

looked at PySpark first but the volume didn't need it. Pandas is fine.

---

## folder structure

    dags/                    Airflow DAG
    src/
      extractors/            reads and normalizes raw files
      transformers/          dedup, ICD-10, anomaly scoring
      loaders/               postgres upsert + ADLS parquet
      utils/                 logger, test data generator
    tests/                   unit tests
    notebooks/               EDA I did before building
    data/raw/                sample CSV for testing
    docs/schema_design.md    schema docs

---

## things I know still need work

Airflow setup is functional but not production-ready. Secrets are in env vars right now instead of Airflow connections. The DAG is more to show the structure than something you'd actually deploy as-is.

Anomaly scoring uses global IQR across all claims which creates false positives. A $40k orthopedic procedure gets flagged even though that's normal for that specialty. Need to group by specialty before scoring, just haven't gotten to it.

Only the transformer has unit tests right now. Extractor and loader need coverage.

Want to replace the pandas transform layer with dbt models at some point.

---

## schema

star schema, full details in docs/schema_design.md

    fact_claims
      claim_id, patient_id, provider_npi, payer_name, service_date,
      icd10_code, cpt_code, claim_amount, approved_amount,
      denial_code, processing_status, anomaly_score

    dim_provider  (npi, name, specialty)
    dim_payer     (payer_name, type)
    dim_date      (date_key, year, quarter, month)

---

phanikumarda@gmail.com
