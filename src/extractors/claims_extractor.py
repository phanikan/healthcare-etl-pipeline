"""
claims_extractor.py
Extract layer of the Healthcare ETL Pipeline.

Handles:
  - CSV / Excel file ingestion with automatic schema detection
  - Multi-file batch processing with glob patterns
  - Raw file archiving with date partitioning
  - Basic structural validation before transformation
  - Azure Data Lake Storage pull (optional)
"""

import os
import shutil
import hashlib
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger("extractor")


# ── Column name normalization map ─────────────────────────────────────────────
# Handles naming variations from different source systems

# Column name normalization — MedCore used "claimid" (no underscore), other sources used
# "claim_number". Took me a while to figure out why the merge was producing so many NaNs
# before I realized they were the same column with different names.
# Added to this list incrementally as we onboarded each new source.
COLUMN_ALIASES = {
    "claimid":          "claim_id",
    "claim_number":     "claim_id",
    "patientid":        "patient_id",
    "member_id":        "patient_id",
    "patientname":      "patient_name",
    "dob":              "patient_dob",
    "birthdate":        "patient_dob",
    "gender":           "patient_gender",
    "sex":              "patient_gender",
    "providername":     "provider_name",
    "physician":        "provider_name",
    "npi":              "provider_npi",
    "insurancepayer":   "payer_name",
    "insurance":        "payer_name",
    "servicedate":      "service_date",
    "date_of_service":  "service_date",
    "submitdate":       "submission_date",
    "icd10":            "icd10_code",
    "diagnosis_code":   "icd10_code",
    "cpt":              "cpt_code",
    "procedure_code":   "cpt_code",
    "amount":           "claim_amount",
    "billed_amount":    "claim_amount",
    "approved":         "approved_amount",
    "paid_amount":      "approved_amount",
    "denial":           "denial_code",
    "status":           "processing_status",
    "claim_status":     "processing_status",
}

REQUIRED_COLUMNS = [
    "claim_id", "patient_id", "service_date",
    "icd10_code", "cpt_code", "claim_amount", "processing_status"
]


class ClaimsExtractor:
    """
    Extracts healthcare claims data from one or more source files.

    Usage:
        extractor = ClaimsExtractor(raw_dir="data/raw", archive_dir="data/archive")
        df, metadata = extractor.extract(source_path="data/raw/claims_20240115.csv")
    """

    def __init__(
        self,
        raw_dir:     str = "data/raw",
        archive_dir: str = "data/archive",
        adls_config: Optional[dict] = None,
    ):
        self.raw_dir     = Path(raw_dir)
        self.archive_dir = Path(archive_dir)
        self.adls_config = adls_config
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    # ── Public API ─────────────────────────────────────────────────────────────

    def extract(self, source_path: str, run_date: Optional[str] = None) -> tuple[pd.DataFrame, dict]:
        """
        Main extraction entry point.

        Args:
            source_path: Path to CSV/Excel file or glob pattern (e.g. "data/raw/*.csv")
            run_date:    Override run date (YYYY-MM-DD). Defaults to today.

        Returns:
            (DataFrame, metadata_dict)
        """
        run_date = run_date or datetime.now().strftime("%Y-%m-%d")
        start_ts = datetime.now()

        logger.pipeline_start(run_date=run_date, source=source_path)

        # Resolve source files
        source = Path(source_path)
        if "*" in str(source_path):
            files = sorted(self.raw_dir.glob(source.name))
        elif source.is_file():
            files = [source]
        else:
            files = sorted(self.raw_dir.glob("*.csv")) + sorted(self.raw_dir.glob("*.xlsx"))

        if not files:
            raise FileNotFoundError(f"No source files found at: {source_path}")

        logger.info(f"Found {len(files)} source file(s): {[f.name for f in files]}")

        # Extract each file
        frames = []
        for f in files:
            df_part = self._read_file(f)
            df_part = self._normalize_columns(df_part)
            df_part = self._add_source_metadata(df_part, f, run_date)
            frames.append(df_part)
            self._archive_file(f, run_date)

        df = pd.concat(frames, ignore_index=True)

        duration = (datetime.now() - start_ts).total_seconds()
        metadata = {
            "run_date":       run_date,
            "source_files":   [str(f) for f in files],
            "rows_extracted": len(df),
            "columns":        list(df.columns),
            "duration_sec":   round(duration, 2),
            "file_hashes":    {f.name: self._file_hash(f) for f in files},
        }

        logger.stage_complete("EXTRACT", records_in=0, records_out=len(df), duration_seconds=duration)
        return df, metadata

    # ── Private helpers ────────────────────────────────────────────────────────

    def _read_file(self, filepath: Path) -> pd.DataFrame:
        """Read CSV or Excel, detect encoding issues gracefully."""
        suffix = filepath.suffix.lower()
        try:
            if suffix == ".csv":
                # Try UTF-8 first, fall back to latin-1
                try:
                    df = pd.read_csv(filepath, dtype=str, low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(filepath, dtype=str, encoding="latin-1", low_memory=False)
            elif suffix in (".xlsx", ".xls"):
                df = pd.read_excel(filepath, dtype=str)
            else:
                raise ValueError(f"Unsupported file format: {suffix}")

            # Strip whitespace from all string columns
            df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
            logger.info(f"Read {len(df):,} rows from {filepath.name}")
            return df

        except Exception as e:
            logger.error(f"Failed to read {filepath.name}: {e}")
            raise

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names using alias map."""
        rename = {}
        for col in df.columns:
            normalized = col.lower().replace(" ", "_").replace("-", "_")
            if normalized in COLUMN_ALIASES:
                rename[col] = COLUMN_ALIASES[normalized]
            elif normalized != col:
                rename[col] = normalized

        if rename:
            df = df.rename(columns=rename)
            logger.debug(f"Renamed columns: {rename}")

        # Warn on missing required columns
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            logger.warning(f"Missing expected columns: {missing}")

        return df

    def _add_source_metadata(self, df: pd.DataFrame, filepath: Path, run_date: str) -> pd.DataFrame:
        """Append pipeline metadata columns for lineage tracking."""
        df["_source_file"] = filepath.name
        df["_run_date"]    = run_date
        df["_ingested_at"] = datetime.utcnow().isoformat()
        return df

    def _archive_file(self, filepath: Path, run_date: str):
        """Move raw file to date-partitioned archive folder.
        
        Note: using copy2 instead of move so the original stays in raw/ until the
        full pipeline run succeeds. If the load fails halfway, we can rerun without
        losing the source. Cleanup of raw/ happens in the Airflow archive task.
        """
        archive_path = self.archive_dir / run_date
        archive_path.mkdir(parents=True, exist_ok=True)
        dest = archive_path / filepath.name
        shutil.copy2(str(filepath), str(dest))
        logger.info(f"Archived {filepath.name} → {dest}")

    @staticmethod
    def _file_hash(filepath: Path) -> str:
        """MD5 hash for file integrity tracking."""
        h = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
