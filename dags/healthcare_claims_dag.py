"""
healthcare_claims_dag.py
Apache Airflow DAG for the Healthcare Claims ETL Pipeline.

Schedule: Daily at 2:00 AM
Retries:  2 attempts with 5-minute delay
SLA:      Must complete within 90 minutes

Task Flow:
    extract_raw_claims
         │
         ▼
    validate_raw_data ──── (fail) ──→ alert_data_quality_failure
         │
         ▼
    transform_claims
         │
         ▼
    detect_anomalies
         │
         ▼
    load_to_warehouse
         │
         ├──→ load_to_azure_adls (parallel)
         │
         ▼
    generate_quality_report
         │
         ▼
    archive_raw_files
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Add project to path
sys.path.insert(0, "/opt/airflow/dags")

from src.extractors.claims_extractor import ClaimsExtractor
from src.transformers.claims_transformer import ClaimsTransformer
from src.loaders.warehouse_loader import WarehouseLoader
from src.utils.logger import get_logger

logger = get_logger("airflow_dag")

# ── DAG default arguments ──────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "phani_kumar",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email":            ["phanikumarda@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "sla":              timedelta(minutes=90),
}

# ── Task functions ─────────────────────────────────────────────────────────────

def extract_task(**context):
    """Extract raw claims from source."""
    run_date = context["ds"]  # Airflow execution date YYYY-MM-DD
    source   = context["params"].get("source_path", "data/raw")

    extractor = ClaimsExtractor()
    raw_df, meta = extractor.extract(source_path=source, run_date=run_date)

    # Push to XCom for downstream tasks
    context["ti"].xcom_push("rows_extracted", meta["rows_extracted"])
    context["ti"].xcom_push("source_files",   meta["source_files"])

    # Save to temp storage
    temp_path = f"/tmp/claims_raw_{run_date}.parquet"
    raw_df.to_parquet(temp_path, index=False)
    context["ti"].xcom_push("raw_path", temp_path)

    logger.info(f"Extract complete: {meta['rows_extracted']:,} rows → {temp_path}")
    return meta["rows_extracted"]


def validate_task(**context):
    """
    Check data quality gates before transformation.
    Returns 'transform_claims' if OK, 'alert_data_quality_failure' if not.
    """
    import pandas as pd

    raw_path      = context["ti"].xcom_pull("extract_raw_claims", key="raw_path")
    rows_extracted = context["ti"].xcom_pull("extract_raw_claims", key="rows_extracted")

    if not raw_path or rows_extracted == 0:
        logger.error("No data extracted — routing to failure alert")
        return "alert_data_quality_failure"

    raw_df = pd.read_parquet(raw_path)

    # Gate 1: Minimum row count
    if len(raw_df) < 100:
        logger.error(f"Row count {len(raw_df)} below minimum threshold 100")
        return "alert_data_quality_failure"

    # Gate 2: Critical columns present
    required = ["claim_id", "claim_amount", "service_date"]
    missing  = [c for c in required if c not in raw_df.columns]
    if missing:
        logger.error(f"Missing critical columns: {missing}")
        return "alert_data_quality_failure"

    # Gate 3: claim_id null rate < 5%
    null_rate = raw_df["claim_id"].isna().mean()
    if null_rate > 0.05:
        logger.error(f"claim_id null rate {null_rate:.1%} exceeds 5% threshold")
        return "alert_data_quality_failure"

    logger.info(f"Validation passed: {len(raw_df):,} rows, all gates OK")
    return "transform_claims"


def transform_task(**context):
    """Transform and clean claims data."""
    import pandas as pd

    run_date = context["ds"]
    raw_path = context["ti"].xcom_pull("extract_raw_claims", key="raw_path")
    raw_df   = pd.read_parquet(raw_path)

    transformer = ClaimsTransformer()
    clean_df, quarantine_df, report = transformer.transform(raw_df)

    clean_path     = f"/tmp/claims_clean_{run_date}.parquet"
    quarantine_path = f"/tmp/claims_quarantine_{run_date}.parquet"
    clean_df.to_parquet(clean_path, index=False)
    if len(quarantine_df):
        quarantine_df.to_parquet(quarantine_path, index=False)

    context["ti"].xcom_push("clean_path",       clean_path)
    context["ti"].xcom_push("quarantine_path",  quarantine_path)
    context["ti"].xcom_push("quality_score",    report["quality_score"])
    context["ti"].xcom_push("rows_clean",        len(clean_df))
    context["ti"].xcom_push("rows_quarantined",  len(quarantine_df))

    return report


def load_warehouse_task(**context):
    """Load clean data to PostgreSQL warehouse."""
    import pandas as pd

    run_date       = context["ds"]
    clean_path     = context["ti"].xcom_pull("transform_claims", key="clean_path")
    quarantine_path = context["ti"].xcom_pull("transform_claims", key="quarantine_path")

    clean_df     = pd.read_parquet(clean_path)
    quarantine_df = pd.read_parquet(quarantine_path) if Path(quarantine_path).exists() else pd.DataFrame()

    loader = WarehouseLoader()
    result = loader.load(
        clean_df=clean_df,
        quarantine_df=quarantine_df,
        run_metadata={
            "run_date":       run_date,
            "rows_extracted": context["ti"].xcom_pull("extract_raw_claims", key="rows_extracted"),
            "quality_score":  context["ti"].xcom_pull("transform_claims", key="quality_score"),
        },
    )

    context["ti"].xcom_push("rows_loaded", result["rows_loaded"])
    return result


def quality_report_task(**context):
    """Generate and log pipeline quality report."""
    run_date        = context["ds"]
    rows_extracted  = context["ti"].xcom_pull("extract_raw_claims", key="rows_extracted")
    rows_clean      = context["ti"].xcom_pull("transform_claims",  key="rows_clean")
    rows_quarantined = context["ti"].xcom_pull("transform_claims", key="rows_quarantined")
    rows_loaded     = context["ti"].xcom_pull("load_to_warehouse", key="rows_loaded")
    quality_score   = context["ti"].xcom_pull("transform_claims",  key="quality_score")

    report = f"""
============================================================
HEALTHCARE CLAIMS ETL — DAILY QUALITY REPORT
Run Date: {run_date}
------------------------------------------------------------
Rows Extracted:    {rows_extracted:>10,}
Rows Clean:        {rows_clean:>10,}
Rows Quarantined:  {rows_quarantined:>10,}
Rows Loaded:       {rows_loaded:>10,}
Quality Score:     {quality_score:>9.1f}%
Status:            ✅ SUCCESS
============================================================
"""
    print(report)
    logger.info("Quality report generated", run_date=run_date, quality_score=quality_score)
    return report


def archive_task(**context):
    """Archive processed files and clean up temp files."""
    import glob, os
    run_date = context["ds"]

    for pattern in [f"/tmp/claims_raw_{run_date}*", f"/tmp/claims_clean_{run_date}*"]:
        for f in glob.glob(pattern):
            try:
                os.remove(f)
                logger.debug(f"Cleaned up temp file: {f}")
            except Exception:
                pass

    logger.info(f"Archive and cleanup complete for {run_date}")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="healthcare_claims_etl",
    description="Daily ETL pipeline for healthcare insurance claims processing",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",   # 2:00 AM daily
    catchup=False,
    tags=["healthcare", "etl", "data-engineering"],
    params={"source_path": "data/raw"},
    doc_md=__doc__,
) as dag:

    # ── Tasks ──────────────────────────────────────────────────────────────────

    extract = PythonOperator(
        task_id="extract_raw_claims",
        python_callable=extract_task,
        provide_context=True,
    )

    validate = BranchPythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_task,
        provide_context=True,
    )

    alert_quality_failure = EmailOperator(
        task_id="alert_data_quality_failure",
        to=["phanikumarda@gmail.com"],
        subject="⚠️ Healthcare ETL: Data Quality Gate Failed — {{ ds }}",
        html_content="""
            <h2>Healthcare Claims ETL — Data Quality Failure</h2>
            <p>The pipeline's data quality validation failed on <b>{{ ds }}</b>.</p>
            <p>Please check the Airflow logs for details.</p>
        """,
    )

    transform = PythonOperator(
        task_id="transform_claims",
        python_callable=transform_task,
        provide_context=True,
    )

    load_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_warehouse_task,
        provide_context=True,
    )

    quality_report = PythonOperator(
        task_id="generate_quality_report",
        python_callable=quality_report_task,
        provide_context=True,
    )

    archive = PythonOperator(
        task_id="archive_raw_files",
        python_callable=archive_task,
        provide_context=True,
    )

    # ── Task dependencies ──────────────────────────────────────────────────────

    extract >> validate >> [alert_quality_failure, transform]
    transform >> load_warehouse >> quality_report >> archive
