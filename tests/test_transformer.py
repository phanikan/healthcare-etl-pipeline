"""
test_transformer.py
Unit tests for the ClaimsTransformer.
Run with: pytest tests/ -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transformers.claims_transformer import ClaimsTransformer


@pytest.fixture
def sample_df():
    """Minimal valid claims DataFrame for testing."""
    return pd.DataFrame({
        "claim_id":         ["CLM-00000001", "CLM-00000002", "CLM-00000003", "CLM-00000004"],
        "patient_id":       ["PAT-1001",      "PAT-1002",     "PAT-1003",     np.nan],
        "provider_npi":     ["1234567890",    "2345678901",   "3456789012",   "4567890123"],
        "payer_name":       ["BlueCross",     "Aetna",        "Medicare",     "Cigna"],
        "service_date":     ["2024-01-15",    "2024-02-20",   "2024-03-10",   "2024-04-05"],
        "submission_date":  ["2024-01-20",    "2024-02-25",   "2024-03-15",   "2024-04-10"],
        "icd10_code":       ["I10",           "E11.9",        "INVALID",      "M54.5"],
        "cpt_code":         ["99213",         "99214",        "85025",        "99203"],
        "claim_amount":     [450.00,          1200.00,        850.00,         300.00],
        "approved_amount":  [405.00,          1100.00,        np.nan,         270.00],
        "denial_code":      [np.nan,          np.nan,         "CO-4",         np.nan],
        "processing_status":["APPROVED",      "A",            "DENIED",       "PENDING"],
    })


@pytest.fixture
def transformer():
    return ClaimsTransformer()


class TestDeduplication:
    def test_removes_exact_duplicates(self, transformer):
        df = pd.DataFrame({
            "claim_id": ["CLM-001", "CLM-001", "CLM-002"],
            "claim_amount": [100.0, 100.0, 200.0],
            "processing_status": ["APPROVED", "APPROVED", "PENDING"],
        })
        clean_df, _, _ = transformer.transform(df)
        assert len(clean_df) == 2
        assert clean_df["claim_id"].nunique() == 2

    def test_keeps_first_occurrence(self, transformer):
        df = pd.DataFrame({
            "claim_id": ["CLM-001", "CLM-001"],
            "claim_amount": [100.0, 999.0],
            "processing_status": ["APPROVED", "DENIED"],
        })
        clean_df, _, _ = transformer.transform(df)
        assert clean_df.iloc[0]["claim_amount"] == 100.0


class TestICD10Validation:
    def test_valid_icd10_passes(self, transformer, sample_df):
        clean_df, _, _ = transformer.transform(sample_df)
        # I10 and E11.9 should be preserved
        assert "I10" in clean_df["icd10_code"].values or "I10" in clean_df["icd10_code"].fillna("").values

    def test_invalid_icd10_nulled(self, transformer, sample_df):
        clean_df, _, _ = transformer.transform(sample_df)
        # "INVALID" should become NaN
        row = clean_df[clean_df["claim_id"] == "CLM-00000003"]
        assert row.empty or pd.isna(row["icd10_code"].values[0])

    def test_icd10_without_dot_corrected(self, transformer):
        df = pd.DataFrame({
            "claim_id": ["CLM-001"],
            "claim_amount": [500.0],
            "icd10_code": ["E119"],  # Should become E11.9
            "processing_status": ["APPROVED"],
        })
        clean_df, _, _ = transformer.transform(df)
        assert clean_df.iloc[0]["icd10_code"] == "E11.9"


class TestStatusNormalization:
    def test_alias_a_becomes_approved(self, transformer, sample_df):
        clean_df, _, _ = transformer.transform(sample_df)
        row = clean_df[clean_df["claim_id"] == "CLM-00000002"]
        if not row.empty:
            assert row.iloc[0]["processing_status"] == "APPROVED"

    def test_unknown_status_becomes_pending(self, transformer):
        df = pd.DataFrame({
            "claim_id": ["CLM-001"],
            "claim_amount": [500.0],
            "processing_status": ["GIBBERISH"],
        })
        clean_df, _, _ = transformer.transform(df)
        assert clean_df.iloc[0]["processing_status"] == "PENDING"


class TestAmountValidation:
    def test_extreme_high_amount_quarantined(self, transformer):
        df = pd.DataFrame({
            "claim_id": ["CLM-001", "CLM-002"],
            "claim_amount": [500.0, 9_999_999.0],  # Second is extreme
            "processing_status": ["APPROVED", "APPROVED"],
        })
        clean_df, quarantine_df, _ = transformer.transform(df)
        assert len(clean_df) == 1
        assert len(quarantine_df) == 1

    def test_zero_amount_quarantined(self, transformer):
        df = pd.DataFrame({
            "claim_id": ["CLM-001"],
            "claim_amount": [0.0],
            "processing_status": ["PENDING"],
        })
        _, quarantine_df, _ = transformer.transform(df)
        assert len(quarantine_df) == 1


class TestAnomalyScoring:
    def test_anomaly_score_column_created(self, transformer, sample_df):
        clean_df, _, _ = transformer.transform(sample_df)
        assert "anomaly_score" in clean_df.columns

    def test_normal_amounts_score_zero(self, transformer):
        df = pd.DataFrame({
            "claim_id":         [f"CLM-{i:04d}" for i in range(100)],
            "claim_amount":     [500.0] * 100,
            "processing_status": ["APPROVED"] * 100,
        })
        clean_df, _, _ = transformer.transform(df)
        assert clean_df["anomaly_score"].max() == 0.0


class TestEnrichment:
    def test_approval_rate_calculated(self, transformer, sample_df):
        clean_df, _, _ = transformer.transform(sample_df)
        assert "approval_rate" in clean_df.columns
        assert clean_df["approval_rate"].between(0.0, 1.0).all()

    def test_transformed_at_timestamp_added(self, transformer, sample_df):
        clean_df, _, _ = transformer.transform(sample_df)
        assert "transformed_at" in clean_df.columns
        assert clean_df["transformed_at"].notna().all()


class TestNullHandling:
    def test_missing_claim_id_quarantined(self, transformer):
        df = pd.DataFrame({
            "claim_id": [np.nan, "CLM-001"],
            "claim_amount": [500.0, 300.0],
            "processing_status": ["APPROVED", "APPROVED"],
        })
        clean_df, quarantine_df, _ = transformer.transform(df)
        assert len(clean_df) == 1
        assert len(quarantine_df) == 1

    def test_approved_amount_null_filled_zero(self, transformer, sample_df):
        clean_df, _, _ = transformer.transform(sample_df)
        assert clean_df["approved_amount"].isna().sum() == 0


class TestQualityReport:
    def test_quality_score_in_report(self, transformer, sample_df):
        _, _, report = transformer.transform(sample_df)
        assert "quality_score" in report
        assert 0 <= report["quality_score"] <= 100

    def test_report_has_all_keys(self, transformer, sample_df):
        _, _, report = transformer.transform(sample_df)
        expected_keys = ["original_count", "clean_count", "quarantine_count", "quality_score"]
        for k in expected_keys:
            assert k in report, f"Missing key: {k}"
