# healthcare-etl-pipeline

Python pipeline for processing healthcare insurance claims. Pulls from CSV exports, cleans and validates the data, loads into PostgreSQL. Also writes parquet files to Azure Data Lake for the BI team.

---

## why I built this

At my last healthcare client I'm calling MedCore, the claims pipeline was basically three SQL scripts running on Windows Task Scheduler with someone checking a report manually every morning. It mostly worked until it didn't.

The real problem wasn't that the data was messy. Healthcare data is always messy. The problem was nobody knew when something failed. A script would die at 2am, no alert, and we'd find out the next afternoon because a claim count looked wrong in a Power BI dashboard. By then the bad records were already downstream.

I built this so there's actual visibility into what's happening. Every run logs what came in, what got filtered and why, and what made it through. Anything the pipeline can't fix goes into a quarantine table with a reason code. Not dropped silently, not buried in a log file nobody reads.

Worth saying upfront: the Airflow part is more illustrative than production ready. I've worked with Airflow on real projects but this DAG would need proper secrets management and a separate metadata database before I'd actually deploy it.

---

## what it does

Takes raw claims CSVs, validates and cleans them, loads them into a star schema in PostgreSQL. Runs daily via Airflow. A parquet copy goes to Azure Data Lake for the BI team to query.

The four things it specifically handles are the ones I kept running into at MedCore.

Duplicate claims come in constantly when you have multiple source systems feeding the same warehouse. The same claim gets submitted twice with slightly different timestamps and the pipeline needs to catch that before load.

ICD-10 formatting issues are usually a system export problem, something like E119 instead of E11.9. The pipeline tries to auto-correct the format. If it can't figure it out it nulls the field and flags the record.

Claim amount outliers get scored using IQR. They don't get dropped because sometimes a $50k claim is legitimate, it just needs a human to review it. The score travels with the record so analysts can filter on it.

Everything else that fails validation goes to the quarantine table with a specific reason code so someone can actually act on it.

---

## numbers from the sample run

```
records in:        10,200
duplicates found:     200  removed before load
bad ICD-10 codes:      51  nulled and flagged
amount outliers:       98  sent to quarantine
anomaly flagged:      441  scored above 0.5, kept in main pipeline
clean records out:  9,902
quality score:      97.1%
runtime:            around 20 seconds
```

I generated synthetic data to match what real MedCore exports looked like, same rough proportions of duplicates and bad codes we used to see.

---

## stack

Python and Pandas for the transform logic. Thought about PySpark but the volume doesn't need it.

Apache Airflow for scheduling, retries, and the email alert that fires when the quality gate fails before transform runs.

PostgreSQL for the warehouse. Star schema because the queries are analytical, not transactional.

Azure Data Lake for the parquet output. Matches what MedCore was using for their BI layer.

Docker Compose so local setup is one command instead of a whole afternoon.

Great Expectations is partially integrated. Still figuring out how to use it inside an Airflow DAG without it generating more noise than signal.

---

## folder structure

```
healthcare-etl-pipeline/
├── dags/
│   └── healthcare_claims_dag.py
├── src/
│   ├── extractors/claims_extractor.py
│   ├── transformers/claims_transformer.py
│   ├── loaders/warehouse_loader.py
│   └── utils/
│       ├── logger.py
│       └── data_generator.py
├── tests/
│   └── test_transformer.py
├── data/raw/claims_sample.csv
├── docs/schema_design.md
├── docker-compose.yml
└── requirements.txt
```

---

## running it locally

You need Docker and Python 3.10 or higher.

```bash
git clone https://github.com/phanikan/healthcare-etl-pipeline
cd healthcare-etl-pipeline

python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python src/utils/data_generator.py --rows 10000

python src/run_pipeline.py --source data/raw/claims_sample.csv --dry-run

cp .env.example .env
docker-compose up -d
```

Airflow runs at localhost:8080. Username and password are both admin.

```bash
pytest tests/ -v
```

17 tests, all passing. They only cover the transformer right now. Extractor and loader tests are still on the list.

---

## what I'd change

The anomaly scoring uses global IQR thresholds right now which is too blunt. A large orthopedic claim looks like an outlier in the model but is completely normal for that claim type. I want to make the thresholds specialty-aware but haven't gotten there yet.

The Airflow DAG needs real secrets management before it's deployable. Right now credentials are just in environment variables which works for local dev but not for anything real.

Things I still want to add: dbt models on top of the warehouse, proper Great Expectations checkpoints wired into the DAG, tests for the extractor and loader, and some kind of simple run history dashboard.

---

## schema

Star schema. Full details in docs/schema_design.md.

```
fact_claims
    ├── dim_provider
    ├── dim_payer
    └── dim_date
```

---

phani kumar · phanikumarda@gmail.com · [kphani.com](https://www.kphani.com)
