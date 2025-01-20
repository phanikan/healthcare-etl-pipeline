"""
data_generator.py
Generates synthetic healthcare claims data for testing and demonstration.
Produces realistic records with intentional data quality issues
(duplicates, nulls, anomalies) to showcase pipeline cleaning capabilities.
"""

import pandas as pd
import numpy as np
import random
import string
import argparse
from datetime import datetime, timedelta
from pathlib import Path


# ── Realistic reference data ──────────────────────────────────────────────────

ICD10_CODES = [
    ("Z00.00", "Encounter for general adult medical exam"),
    ("I10",    "Essential (primary) hypertension"),
    ("E11.9",  "Type 2 diabetes mellitus without complications"),
    ("J06.9",  "Acute upper respiratory infection, unspecified"),
    ("M54.5",  "Low back pain"),
    ("F32.9",  "Major depressive disorder, single episode"),
    ("K21.0",  "Gastro-esophageal reflux disease with esophagitis"),
    ("N39.0",  "Urinary tract infection, site not specified"),
    ("J18.9",  "Pneumonia, unspecified organism"),
    ("I25.10", "Atherosclerotic heart disease of native coronary artery"),
]

CPT_CODES = [
    ("99213", "Office visit, established patient, low complexity"),
    ("99214", "Office visit, established patient, moderate complexity"),
    ("99203", "Office visit, new patient, low complexity"),
    ("93000", "Electrocardiogram, routine ECG with at least 12 leads"),
    ("85025", "Complete blood count (CBC), automated"),
    ("80053", "Comprehensive metabolic panel"),
    ("71046", "Chest X-ray, 2 views"),
    ("99232", "Subsequent hospital care, low complexity"),
    ("36415", "Collection of venous blood by venipuncture"),
    ("99395", "Preventive medicine exam, 18-39 years"),
]

PAYERS = ["BlueCross BlueShield", "Aetna", "Cigna", "UnitedHealth", "Humana", "Medicare", "Medicaid"]
PROVIDERS = [
    ("Dr. Sarah Johnson", "1234567890", "Internal Medicine"),
    ("Dr. Michael Chen",  "2345678901", "Cardiology"),
    ("Dr. Emily Davis",   "3456789012", "Family Medicine"),
    ("Dr. Robert Wilson", "4567890123", "Orthopedics"),
    ("Dr. Lisa Martinez", "5678901234", "Psychiatry"),
]
DENIAL_CODES = [None, None, None, None, "CO-4", "CO-11", "CO-97", "PR-1", "OA-23"]
STATUSES    = ["APPROVED", "APPROVED", "APPROVED", "PENDING", "DENIED", "PARTIALLY_APPROVED"]


def _random_date(start_year: int = 2023, end_year: int = 2024) -> str:
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")


def _random_dob() -> str:
    age_days = random.randint(365 * 18, 365 * 85)
    return (datetime.now() - timedelta(days=age_days)).strftime("%Y-%m-%d")


def _random_npi() -> str:
    return "".join([str(random.randint(0, 9)) for _ in range(10)])


def _claim_id(i: int) -> str:
    return f"CLM-{i:08d}"


def generate_claims(n_rows: int = 10_000, seed: int = 42) -> pd.DataFrame:
    """
    Generate n_rows synthetic claims records.
    Intentionally injects:
      - ~2% duplicates
      - ~3% null critical fields
      - ~1% anomalous claim amounts (statistical outliers)
      - ~0.5% invalid ICD-10 formats
    """
    random.seed(seed)
    np.random.seed(seed)

    records = []
    for i in range(1, n_rows + 1):
        icd   = random.choice(ICD10_CODES)
        cpt   = random.choice(CPT_CODES)
        prov  = random.choice(PROVIDERS)
        payer = random.choice(PAYERS)

        # Claim amount: log-normal to mimic real healthcare billing
        amount = round(np.random.lognormal(mean=6.5, sigma=1.2), 2)
        amount = max(10.0, min(amount, 250_000.0))

        approved = round(amount * random.uniform(0.70, 1.0), 2)
        status   = random.choice(STATUSES)
        denial   = random.choice(DENIAL_CODES)

        records.append({
            "claim_id":          _claim_id(i),
            "patient_id":        f"PAT-{random.randint(10000, 99999)}",
            "patient_name":      f"Patient_{random.randint(1000, 9999)}",
            "patient_dob":       _random_dob(),
            "patient_gender":    random.choice(["M", "F", "M", "F", "U"]),
            "provider_name":     prov[0],
            "provider_npi":      prov[1],
            "provider_specialty":prov[2],
            "payer_name":        payer,
            "service_date":      _random_date(),
            "submission_date":   _random_date(),
            "icd10_code":        icd[0],
            "icd10_description": icd[1],
            "cpt_code":          cpt[0],
            "cpt_description":   cpt[1],
            "claim_amount":      amount,
            "approved_amount":   approved if status != "DENIED" else 0.0,
            "denial_code":       denial if status == "DENIED" else None,
            "processing_status": status,
        })

    df = pd.DataFrame(records)

    # ── Inject data quality issues ─────────────────────────────────────────────
    n = len(df)

    # 2% duplicates
    dup_idx = df.sample(frac=0.02, random_state=seed).index
    duplicates = df.loc[dup_idx].copy()
    df = pd.concat([df, duplicates], ignore_index=True)

    # 3% null patient_id
    null_idx = df.sample(frac=0.03, random_state=seed + 1).index
    df.loc[null_idx, "patient_id"] = np.nan

    # 1% extreme outlier amounts
    outlier_idx = df.sample(frac=0.01, random_state=seed + 2).index
    df.loc[outlier_idx, "claim_amount"] = np.random.uniform(500_000, 2_000_000, len(outlier_idx))

    # 0.5% bad ICD-10 codes
    bad_icd_idx = df.sample(frac=0.005, random_state=seed + 3).index
    bad_values = (["INVALID", "XXX", "???", ""] * (len(bad_icd_idx) // 4 + 2))[:len(bad_icd_idx)]
    df.loc[bad_icd_idx, "icd10_code"] = bad_values

    df = df.sample(frac=1, random_state=seed).reset_index(drop=True)  # shuffle
    return df


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic healthcare claims data")
    parser.add_argument("--rows",   type=int, default=10_000, help="Number of base records")
    parser.add_argument("--output", type=str, default="data/raw/claims_sample.csv")
    parser.add_argument("--seed",   type=int, default=42)
    args = parser.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    print(f"⚙️  Generating {args.rows:,} synthetic claims records...")
    df = generate_claims(n_rows=args.rows, seed=args.seed)
    df.to_csv(args.output, index=False)
    print(f"✅ Saved {len(df):,} records (incl. injected duplicates) → {args.output}")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Sample:\n{df.head(3).to_string()}")


if __name__ == "__main__":
    main()
