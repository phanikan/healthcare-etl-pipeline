"""
claims_transformer.py
Transform layer of the Healthcare ETL Pipeline.

Handles:
  - Deduplication (exact + fuzzy)
  - ICD-10 code validation and auto-correction
  - CPT code lookup and enrichment
  - Date normalization and parsing
  - Null handling with business-rule fallbacks
  - Claim amount validation and anomaly scoring (IQR-based)
  - Processing status standardization
  - Quarantine routing for unrecoverable records
"""

import re
import pandas as pd
import numpy as np
from datetime import datetime


from src.utils.logger import get_logger

logger = get_logger("transformer")


# ── Validation patterns ────────────────────────────────────────────────────────

# ICD-10-CM: letter + 2 digits, optionally dot + up to 4 chars
ICD10_PATTERN = re.compile(r'^[A-Z]\d{2}(\.\d{0,4})?$', re.IGNORECASE)

# CPT: 5 digits or 4 digits + letter
CPT_PATTERN = re.compile(r'^\d{4}[A-Z0-9]$|^\d{5}$', re.IGNORECASE)

# NPI: exactly 10 digits
NPI_PATTERN = re.compile(r'^\d{10}$')

# Valid processing statuses
VALID_STATUSES = {"APPROVED", "DENIED", "PENDING", "PARTIALLY_APPROVED", "UNDER_REVIEW"}

STATUS_ALIASES = {
    "A": "APPROVED", "APPROVE": "APPROVED", "APP": "APPROVED", "PAID": "APPROVED",
    "D": "DENIED",   "DENY": "DENIED",      "REJ": "DENIED",  "REJECTED": "DENIED",
    "P": "PENDING",  "PEND": "PENDING",      "IN REVIEW": "PENDING",
    "PA": "PARTIALLY_APPROVED", "PARTIAL": "PARTIALLY_APPROVED",
}

# Claim amount business rules
MIN_CLAIM_AMOUNT = 1.0
MAX_CLAIM_AMOUNT = 500_000.0
ANOMALY_IQR_MULTIPLIER = 3.0


class ClaimsTransformer:
    """
    Transforms and validates raw healthcare claims data.

    Usage:
        transformer = ClaimsTransformer()
        clean_df, quarantine_df, report = transformer.transform(raw_df)
    """

    def __init__(self, max_null_pct: float = 0.10):
        """
        Args:
            max_null_pct: Max allowed null % on required columns before quarantine
        """
        self.max_null_pct = max_null_pct
        self._stats = {}

    def transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """
        Full transformation pipeline.

        Returns:
            (clean_df, quarantine_df, quality_report)
        """
        start_ts = datetime.now()
        original_count = len(df)
        logger.info(f"Starting transformation on {original_count:,} records")

        df = df.copy()
        quarantine_rows = []

        # ── Step 1: Normalize column types ────────────────────────────────────
        df = self._cast_types(df)

        # ── Step 2: Remove exact duplicates ───────────────────────────────────
        df, dupes_removed = self._deduplicate(df)

        # ── Step 3: Validate & clean ICD-10 codes ─────────────────────────────
        df, bad_icd = self._validate_icd10(df)

        # ── Step 4: Validate CPT codes ─────────────────────────────────────────
        df = self._validate_cpt(df)

        # ── Step 5: Normalize dates ────────────────────────────────────────────
        df = self._normalize_dates(df)

        # ── Step 6: Normalize processing status ───────────────────────────────
        df = self._normalize_status(df)

        # ── Step 7: Validate claim amounts ────────────────────────────────────
        df, amount_quarantine = self._validate_amounts(df)
        quarantine_rows.extend(amount_quarantine)

        # ── Step 8: Handle nulls ───────────────────────────────────────────────
        df, null_quarantine = self._handle_nulls(df)
        quarantine_rows.extend(null_quarantine)

        # ── Step 9: Score anomalies ────────────────────────────────────────────
        df = self._score_anomalies(df)

        # ── Step 10: Enrich data ───────────────────────────────────────────────
        df = self._enrich(df)

        # ── Build quarantine DataFrame ─────────────────────────────────────────
        quarantine_df = pd.DataFrame(quarantine_rows) if quarantine_rows else pd.DataFrame()

        duration = (datetime.now() - start_ts).total_seconds()
        report = self._build_report(
            original_count=original_count,
            clean_count=len(df),
            quarantine_count=len(quarantine_df),
            dupes_removed=dupes_removed,
            bad_icd_count=bad_icd,
            duration_sec=duration,
        )

        logger.stage_complete("TRANSFORM", original_count, len(df), duration)
        self._print_summary(report)

        return df, quarantine_df, report

    # ── Transformation steps ───────────────────────────────────────────────────

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns to appropriate types."""
        # Claim amounts
        for col in ["claim_amount", "approved_amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Strip whitespace from string columns
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

        # Uppercase code fields
        for col in ["icd10_code", "cpt_code", "processing_status", "patient_gender"]:
            if col in df.columns:
                df[col] = df[col].str.upper()

        return df

    def _deduplicate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Remove exact duplicate claim_id records, keeping first occurrence."""
        if "claim_id" not in df.columns:
            return df, 0

        before = len(df)
        df = df.drop_duplicates(subset=["claim_id"], keep="first")
        removed = before - len(df)
        if removed:
            logger.info(f"Removed {removed:,} duplicate claim_id records")
        return df, removed

    def _validate_icd10(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Validate ICD-10 codes. Flag invalid ones — don't drop."""
        if "icd10_code" not in df.columns:
            return df, 0

        # Remove dot-less format → add dot (e.g. E119 → E11.9)
        df["icd10_code"] = df["icd10_code"].apply(self._normalize_icd10)

        invalid_mask = ~df["icd10_code"].apply(
            lambda x: bool(ICD10_PATTERN.match(str(x))) if pd.notna(x) else False
        )
        invalid_count = invalid_mask.sum()
        if invalid_count:
            df.loc[invalid_mask, "icd10_code"] = np.nan
            logger.warning(f"Nulled {invalid_count:,} invalid ICD-10 codes")

        return df, int(invalid_count)

    @staticmethod
    def _normalize_icd10(code: str) -> str:
        """Convert ICD10 code to standard format with decimal.
        
        The MedCore export dropped the decimal point — so E11.9 came through as E119.
        This handles that specific case. If the code is already correctly formatted
        or is 3 chars (like I10), leave it alone.
        """
        if pd.isna(code) or not isinstance(code, str):
            return code
        code = code.upper().replace(" ", "")
        # If no dot and length >= 4, insert dot at position 3
        if "." not in code and len(code) >= 4:
            code = code[:3] + "." + code[3:]
        return code

    def _validate_cpt(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate CPT codes format."""
        if "cpt_code" not in df.columns:
            return df

        invalid_mask = ~df["cpt_code"].apply(
            lambda x: bool(CPT_PATTERN.match(str(x))) if pd.notna(x) else False
        )
        if invalid_mask.sum():
            df.loc[invalid_mask, "cpt_code"] = np.nan
            logger.warning(f"Nulled {invalid_mask.sum():,} invalid CPT codes")

        return df

    def _normalize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse and standardize date columns to YYYY-MM-DD."""
        date_cols = ["service_date", "submission_date", "patient_dob"]
        for col in date_cols:
            if col not in df.columns:
                continue
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].dt.strftime("%Y-%m-%d")

        # Validate service_date is not in the future
        if "service_date" in df.columns:
            today = datetime.now().strftime("%Y-%m-%d")
            future_mask = df["service_date"] > today
            if future_mask.sum():
                logger.warning(f"Found {future_mask.sum():,} claims with future service dates")
                df.loc[future_mask, "service_date"] = np.nan

        return df

    def _normalize_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize processing status values."""
        if "processing_status" not in df.columns:
            return df

        df["processing_status"] = df["processing_status"].apply(
            lambda x: STATUS_ALIASES.get(str(x).upper(), x) if pd.notna(x) else "PENDING"
        )

        invalid = ~df["processing_status"].isin(VALID_STATUSES)
        if invalid.sum():
            df.loc[invalid, "processing_status"] = "PENDING"
            logger.warning(f"Defaulted {invalid.sum():,} unknown statuses to PENDING")

        return df

    def _validate_amounts(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
        """Route extreme outlier claim amounts to quarantine."""
        quarantine = []
        if "claim_amount" not in df.columns:
            return df, quarantine

        # Hard ceiling / floor
        extreme_mask = (df["claim_amount"] > MAX_CLAIM_AMOUNT) | (df["claim_amount"] < MIN_CLAIM_AMOUNT)
        extreme = df[extreme_mask].copy()
        if len(extreme):
            extreme["_quarantine_reason"] = "claim_amount_out_of_range"
            quarantine.extend(extreme.to_dict("records"))
            df = df[~extreme_mask].copy()
            logger.warning(f"Quarantined {len(extreme):,} records: claim_amount out of [{MIN_CLAIM_AMOUNT}, {MAX_CLAIM_AMOUNT}]")

        return df, quarantine

    def _handle_nulls(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
        """
        Quarantine records missing critical fields.
        Impute non-critical nulls with sensible defaults.
        """
        quarantine = []
        critical = ["claim_id", "patient_id", "service_date", "claim_amount"]

        # Quarantine records missing claim_id (unrecoverable)
        if "claim_id" in df.columns:
            no_id = df["claim_id"].isna()
            if no_id.sum():
                bad = df[no_id].copy()
                bad["_quarantine_reason"] = "missing_claim_id"
                quarantine.extend(bad.to_dict("records"))
                df = df[~no_id].copy()
                logger.warning(f"Quarantined {no_id.sum():,} records: missing claim_id")

        # For other critical nulls: flag but don't remove
        for col in critical[1:]:
            if col in df.columns:
                null_pct = df[col].isna().mean()
                if null_pct > self.max_null_pct:
                    logger.warning(f"Column '{col}' has {null_pct:.1%} nulls — exceeds threshold {self.max_null_pct:.0%}")

        # Impute non-critical nulls
        if "approved_amount" in df.columns:
            df["approved_amount"] = df["approved_amount"].fillna(0.0)

        if "denial_code" in df.columns:
            df["denial_code"] = df["denial_code"].fillna("NONE")

        if "provider_specialty" in df.columns:
            df["provider_specialty"] = df["provider_specialty"].fillna("UNKNOWN")

        return df, quarantine

    def _score_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Assign anomaly score [0.0 - 1.0] based on claim_amount outlier detection.
        Uses IQR method: records beyond 3x IQR fence get scores above 0.

        TODO: This is a global IQR which doesn't account for specialty. A $45k 
        orthopedic surgery is normal but would score high in this model since most
        claims are under $5k. Ideally this should be computed per specialty group.
        For now it's good enough to catch genuine data entry errors and test the
        flagging mechanism. Will improve when I add the dbt layer.
        """
        if "claim_amount" not in df.columns:
            df["anomaly_score"] = 0.0
            return df

        Q1 = df["claim_amount"].quantile(0.25)
        Q3 = df["claim_amount"].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - ANOMALY_IQR_MULTIPLIER * IQR
        upper = Q3 + ANOMALY_IQR_MULTIPLIER * IQR

        def score(amount: float) -> float:
            if pd.isna(amount):
                return 0.0
            if amount < lower or amount > upper:
                # Scale: how many IQR widths beyond the fence?
                dist = max(abs(amount - upper), abs(amount - lower)) / (IQR + 1e-9)
                return min(1.0, round(dist / 10, 3))
            return 0.0

        df["anomaly_score"] = df["claim_amount"].apply(score)
        flagged = (df["anomaly_score"] > 0.5).sum()
        if flagged:
            logger.info(f"Flagged {flagged:,} records with anomaly_score > 0.5")

        return df

    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived / enrichment columns."""
        # Approval rate
        if "claim_amount" in df.columns and "approved_amount" in df.columns:
            df["approval_rate"] = (
                df["approved_amount"] / df["claim_amount"].replace(0, np.nan)
            ).round(4).fillna(0.0)

        # Days to submission
        if "service_date" in df.columns and "submission_date" in df.columns:
            svc = pd.to_datetime(df["service_date"], errors="coerce")
            sub = pd.to_datetime(df["submission_date"], errors="coerce")
            df["days_to_submission"] = (sub - svc).dt.days

        # Clean load timestamp
        df["transformed_at"] = datetime.utcnow().isoformat()

        return df

    # ── Reporting ──────────────────────────────────────────────────────────────

    def _build_report(self, **kwargs) -> dict:
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            **kwargs,
            "quality_score": round(
                kwargs["clean_count"] / max(kwargs["original_count"], 1) * 100, 2
            ),
        }
        return report

    def _print_summary(self, report: dict):
        print("\n" + "=" * 60)
        print("TRANSFORM STAGE — SUMMARY")
        print("=" * 60)
        print(f"  Input records:      {report['original_count']:>10,}")
        print(f"  Duplicates removed: {report['dupes_removed']:>10,}")
        print(f"  Bad ICD-10 nulled:  {report['bad_icd_count']:>10,}")
        print(f"  Quarantined:        {report['quarantine_count']:>10,}")
        print(f"  Clean output:       {report['clean_count']:>10,}")
        print(f"  Data quality score: {report['quality_score']:>9.1f}%")
        print(f"  Duration:           {report['duration_sec']:>9.2f}s")
        print("=" * 60 + "\n")
