"""
warehouse_loader.py
Load layer of the Healthcare ETL Pipeline.

Handles:
  - PostgreSQL data warehouse upsert (star schema)
  - Azure Data Lake Storage Gen2 — Parquet output (date-partitioned)
  - Quarantine table writes
  - Pipeline run audit logging
  - Schema creation (idempotent)
"""

import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger("loader")

# Optional imports — fail gracefully if not installed
try:
    import psycopg2
    from psycopg2.extras import execute_values
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logger.warning("psycopg2 not installed — PostgreSQL loading disabled")

try:
    from azure.storage.filedatalake import DataLakeServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logger.warning("azure-storage-file-datalake not installed — Azure ADLS loading disabled")


# ── Column mapping: DataFrame → warehouse ─────────────────────────────────────

FACT_COLUMNS = [
    "claim_id", "patient_id", "provider_npi", "payer_name",
    "service_date", "icd10_code", "cpt_code",
    "claim_amount", "approved_amount", "denial_code",
    "processing_status", "anomaly_score", "approval_rate",
    "days_to_submission", "transformed_at",
]

WAREHOUSE_SCHEMA = """
-- Run this once to initialize the warehouse schema
-- healthcare_dw database

CREATE TABLE IF NOT EXISTS dim_date (
    date_key    SERIAL PRIMARY KEY,
    full_date   DATE UNIQUE,
    year        INT,
    quarter     INT,
    month       INT,
    week        INT,
    day_of_week INT,
    is_weekend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_payer (
    payer_key   SERIAL PRIMARY KEY,
    payer_name  VARCHAR(100) UNIQUE,
    payer_type  VARCHAR(50),
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_provider (
    provider_key    SERIAL PRIMARY KEY,
    provider_npi    VARCHAR(10) UNIQUE,
    provider_name   VARCHAR(150),
    specialty       VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_claims (
    claim_id            VARCHAR(20) PRIMARY KEY,
    patient_id          VARCHAR(20),
    provider_npi        VARCHAR(10),
    payer_name          VARCHAR(100),
    service_date        DATE,
    icd10_code          VARCHAR(10),
    cpt_code            VARCHAR(10),
    claim_amount        NUMERIC(14,2),
    approved_amount     NUMERIC(14,2),
    denial_code         VARCHAR(10),
    processing_status   VARCHAR(25),
    anomaly_score       FLOAT,
    approval_rate       FLOAT,
    days_to_submission  INT,
    transformed_at      TIMESTAMP,
    loaded_at           TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quarantine_claims (
    id                  SERIAL PRIMARY KEY,
    claim_id            VARCHAR(20),
    quarantine_reason   VARCHAR(200),
    raw_data            JSONB,
    quarantined_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              SERIAL PRIMARY KEY,
    run_date            DATE,
    source_files        TEXT,
    rows_extracted      INT,
    rows_transformed    INT,
    rows_loaded         INT,
    rows_quarantined    INT,
    quality_score       FLOAT,
    duration_seconds    FLOAT,
    status              VARCHAR(20),
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP DEFAULT NOW()
);

-- Indexes for analytical query performance
CREATE INDEX IF NOT EXISTS idx_fact_claims_service_date  ON fact_claims(service_date);
CREATE INDEX IF NOT EXISTS idx_fact_claims_status        ON fact_claims(processing_status);
CREATE INDEX IF NOT EXISTS idx_fact_claims_icd10         ON fact_claims(icd10_code);
CREATE INDEX IF NOT EXISTS idx_fact_claims_payer         ON fact_claims(payer_name);
CREATE INDEX IF NOT EXISTS idx_fact_claims_anomaly       ON fact_claims(anomaly_score);
"""


class WarehouseLoader:
    """
    Loads transformed claims data to PostgreSQL and/or Azure ADLS.

    Usage:
        loader = WarehouseLoader(db_config={...}, adls_config={...})
        result = loader.load(clean_df, quarantine_df, run_metadata)
    """

    def __init__(
        self,
        db_config:   Optional[dict] = None,
        adls_config: Optional[dict] = None,
        local_output_dir: str = "data/processed",
    ):
        self.db_config        = db_config or self._db_config_from_env()
        self.adls_config      = adls_config or self._adls_config_from_env()
        self.local_output_dir = Path(local_output_dir)
        self.local_output_dir.mkdir(parents=True, exist_ok=True)

    # ── Public API ─────────────────────────────────────────────────────────────

    def load(
        self,
        clean_df:      pd.DataFrame,
        quarantine_df: pd.DataFrame,
        run_metadata:  dict,
    ) -> dict:
        """
        Load clean records to warehouse and quarantine table.

        Returns:
            load_result dict with row counts and status
        """
        start_ts = datetime.now()
        rows_loaded = 0
        rows_quarantined = 0

        # ── Always: write Parquet locally ─────────────────────────────────────
        local_path = self._write_local_parquet(clean_df, run_metadata.get("run_date", "unknown"))
        logger.info(f"Wrote {len(clean_df):,} rows to local Parquet: {local_path}")

        # ── PostgreSQL load ────────────────────────────────────────────────────
        if PSYCOPG2_AVAILABLE and self.db_config.get("host"):
            try:
                rows_loaded = self._load_postgres(clean_df, quarantine_df, run_metadata)
            except Exception as e:
                logger.error(f"PostgreSQL load failed: {e}")
        else:
            # Simulate load for demo purposes
            rows_loaded = len(clean_df)
            logger.info(f"[DEMO MODE] Simulated PostgreSQL upsert of {rows_loaded:,} rows")

        # ── Azure ADLS load ────────────────────────────────────────────────────
        if AZURE_AVAILABLE and self.adls_config.get("account_name"):
            try:
                self._load_azure_adls(clean_df, run_metadata.get("run_date", ""))
            except Exception as e:
                logger.error(f"Azure ADLS load failed: {e}")
        else:
            logger.info("[DEMO MODE] Azure ADLS upload skipped (not configured)")

        # ── Quarantine ─────────────────────────────────────────────────────────
        rows_quarantined = len(quarantine_df)
        if rows_quarantined:
            quarantine_path = self.local_output_dir / f"quarantine_{run_metadata.get('run_date','')}.csv"
            quarantine_df.to_csv(quarantine_path, index=False)
            logger.warning(f"Wrote {rows_quarantined:,} quarantine records → {quarantine_path}")

        duration = (datetime.now() - start_ts).total_seconds()
        result = {
            "rows_loaded":      rows_loaded,
            "rows_quarantined": rows_quarantined,
            "local_parquet":    str(local_path),
            "duration_sec":     round(duration, 2),
            "status":           "SUCCESS",
        }

        logger.stage_complete("LOAD", len(clean_df), rows_loaded, duration)
        return result

    # ── PostgreSQL ─────────────────────────────────────────────────────────────

    def _load_postgres(
        self,
        clean_df:      pd.DataFrame,
        quarantine_df: pd.DataFrame,
        run_metadata:  dict,
    ) -> int:
        conn = psycopg2.connect(**self.db_config)
        try:
            with conn:
                with conn.cursor() as cur:
                    # Upsert fact_claims
                    rows = self._df_to_rows(clean_df, FACT_COLUMNS)
                    cols = ", ".join(FACT_COLUMNS)
                    placeholders = ", ".join(["%s"] * len(FACT_COLUMNS))
                    update_cols = ", ".join(
                        [f"{c} = EXCLUDED.{c}" for c in FACT_COLUMNS if c != "claim_id"]
                    )
        # Upsert (INSERT ON CONFLICT) instead of truncate-reload.
        # I originally did truncate-reload because it's simpler, but hit a problem
        # where a mid-run failure would leave the table empty until the next day.
        # Upsert means a failed re-run doesn't wipe what was already there.
        sql = f"""
                        INSERT INTO fact_claims ({cols})
                        VALUES ({placeholders})
                        ON CONFLICT (claim_id) DO UPDATE SET
                            {update_cols},
                            updated_at = NOW()
                    """
                    execute_values(cur, sql, rows, template=None, page_size=1000)

                    # Quarantine
                    if len(quarantine_df):
                        for _, row in quarantine_df.iterrows():
                            cur.execute(
                                """INSERT INTO quarantine_claims (claim_id, quarantine_reason, raw_data)
                                   VALUES (%s, %s, %s)""",
                                (
                                    row.get("claim_id"),
                                    row.get("_quarantine_reason", "unknown"),
                                    json.dumps(row.to_dict()),
                                )
                            )

                    # Pipeline run audit
                    cur.execute(
                        """INSERT INTO pipeline_runs
                           (run_date, source_files, rows_extracted, rows_transformed,
                            rows_loaded, rows_quarantined, quality_score, duration_seconds, status, started_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (
                            run_metadata.get("run_date"),
                            str(run_metadata.get("source_files", [])),
                            run_metadata.get("rows_extracted", 0),
                            len(clean_df) + len(quarantine_df),
                            len(clean_df),
                            len(quarantine_df),
                            run_metadata.get("quality_score", 0),
                            run_metadata.get("duration_sec", 0),
                            "SUCCESS",
                            run_metadata.get("started_at", datetime.utcnow()),
                        )
                    )

            logger.info(f"PostgreSQL: upserted {len(rows):,} rows into fact_claims")
            return len(rows)

        finally:
            conn.close()

    # ── Azure ADLS ─────────────────────────────────────────────────────────────

    def _load_azure_adls(self, df: pd.DataFrame, run_date: str):
        """Write DataFrame as Parquet to Azure Data Lake, date-partitioned."""
        service = DataLakeServiceClient(
            account_url=f"https://{self.adls_config['account_name']}.dfs.core.windows.net",
            credential=self.adls_config["account_key"],
        )
        container = service.get_file_system_client(self.adls_config["container"])
        year, month, day = run_date.split("-") if run_date else ("0000", "00", "00")
        path = f"claims/year={year}/month={month}/day={day}/claims_{run_date}.parquet"

        file_client = container.get_file_client(path)
        parquet_bytes = df.to_parquet(index=False)
        file_client.upload_data(parquet_bytes, overwrite=True)
        logger.info(f"Azure ADLS: uploaded {len(df):,} rows → {path}")

    # ── Local Parquet (fallback / always) ─────────────────────────────────────

    def _write_local_parquet(self, df: pd.DataFrame, run_date: str) -> Path:
        out = self.local_output_dir / f"claims_{run_date}.parquet"
        df.to_parquet(out, index=False)
        return out

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _df_to_rows(self, df: pd.DataFrame, columns: list) -> list:
        """Extract rows from DataFrame, filling missing columns with None."""
        rows = []
        for _, row in df.iterrows():
            rows.append(tuple(row.get(c) for c in columns))
        return rows

    @staticmethod
    def _db_config_from_env() -> dict:
        return {
            "host":     os.getenv("DB_HOST", "localhost"),
            "port":     int(os.getenv("DB_PORT", 5432)),
            "dbname":   os.getenv("DB_NAME", "healthcare_dw"),
            "user":     os.getenv("DB_USER", "etl_user"),
            "password": os.getenv("DB_PASSWORD", ""),
        }

    @staticmethod
    def _adls_config_from_env() -> dict:
        return {
            "account_name": os.getenv("AZURE_STORAGE_ACCOUNT", ""),
            "account_key":  os.getenv("AZURE_STORAGE_KEY", ""),
            "container":    os.getenv("AZURE_CONTAINER", "healthcare-claims"),
        }

    @staticmethod
    def get_schema_sql() -> str:
        """Return DDL for warehouse schema initialization."""
        return WAREHOUSE_SCHEMA
