"""
run_pipeline.py
Main entry point for the Healthcare Claims ETL Pipeline.
Can be run directly (CLI) or called from the Airflow DAG.

Usage:
    python src/run_pipeline.py
    python src/run_pipeline.py --date 2024-01-15 --source data/raw/claims_20240115.csv
    python src/run_pipeline.py --dry-run
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extractors.claims_extractor import ClaimsExtractor
from src.transformers.claims_transformer import ClaimsTransformer
from src.loaders.warehouse_loader import WarehouseLoader
from src.utils.logger import get_logger

logger = get_logger("pipeline")


def run_pipeline(
    source_path: str = "data/raw",
    run_date:    str = None,
    dry_run:     bool = False,
) -> dict:
    """
    Execute the full ETL pipeline: Extract → Transform → Load.

    Args:
        source_path: Path to source file(s)
        run_date:    Processing date (YYYY-MM-DD), defaults to today
        dry_run:     If True, skip the load step

    Returns:
        Pipeline run summary dict
    """
    pipeline_start = datetime.now()
    run_date = run_date or datetime.now().strftime("%Y-%m-%d")

    print("\n" + "=" * 60)
    print("  HEALTHCARE CLAIMS ETL PIPELINE")
    print(f"  Run Date: {run_date}  |  Mode: {'DRY RUN' if dry_run else 'FULL'}")
    print("=" * 60)

    try:
        # ── EXTRACT ────────────────────────────────────────────────────────────
        print("\n[1/3] EXTRACT — Ingesting source data...")
        extractor = ClaimsExtractor()
        raw_df, extract_meta = extractor.extract(source_path=source_path, run_date=run_date)
        print(f"      ✓ {len(raw_df):,} records extracted from {len(extract_meta['source_files'])} file(s)")

        # ── TRANSFORM ──────────────────────────────────────────────────────────
        print("\n[2/3] TRANSFORM — Cleaning and validating...")
        transformer = ClaimsTransformer()
        clean_df, quarantine_df, transform_report = transformer.transform(raw_df)
        print(f"      ✓ {len(clean_df):,} clean records | {len(quarantine_df):,} quarantined")
        print(f"      ✓ Data quality score: {transform_report['quality_score']:.1f}%")

        # ── LOAD ───────────────────────────────────────────────────────────────
        if not dry_run:
            print("\n[3/3] LOAD — Writing to warehouse...")
            loader = WarehouseLoader()
            load_result = loader.load(
                clean_df=clean_df,
                quarantine_df=quarantine_df,
                run_metadata={
                    **extract_meta,
                    **transform_report,
                    "started_at": pipeline_start,
                },
            )
            print(f"      ✓ {load_result['rows_loaded']:,} rows loaded to warehouse")
            print(f"      ✓ Parquet saved: {load_result['local_parquet']}")
        else:
            print("\n[3/3] LOAD — Skipped (dry run mode)")
            load_result = {"rows_loaded": 0, "status": "DRY_RUN"}

        # ── SUMMARY ────────────────────────────────────────────────────────────
        total_duration = (datetime.now() - pipeline_start).total_seconds()
        summary = {
            "status":           "SUCCESS",
            "run_date":         run_date,
            "rows_extracted":   extract_meta["rows_extracted"],
            "rows_clean":       len(clean_df),
            "rows_quarantined": len(quarantine_df),
            "rows_loaded":      load_result["rows_loaded"],
            "quality_score":    transform_report["quality_score"],
            "duration_sec":     round(total_duration, 1),
        }

        _print_final_summary(summary)
        logger.pipeline_end(
            records_processed=summary["rows_loaded"],
            duration_seconds=total_duration,
            status="SUCCESS",
        )
        return summary

    except Exception as e:
        duration = (datetime.now() - pipeline_start).total_seconds()
        logger.error(f"Pipeline FAILED after {duration:.1f}s: {e}")
        print(f"\n❌ PIPELINE FAILED: {e}")
        raise


def _print_final_summary(summary: dict):
    print("\n" + "=" * 60)
    print("  PIPELINE RUN SUMMARY")
    print("=" * 60)
    print(f"  Run Date:          {summary['run_date']}")
    print(f"  Rows Extracted:    {summary['rows_extracted']:>10,}")
    print(f"  Rows Clean:        {summary['rows_clean']:>10,}")
    print(f"  Rows Quarantined:  {summary['rows_quarantined']:>10,}")
    print(f"  Rows Loaded:       {summary['rows_loaded']:>10,}")
    print(f"  Quality Score:     {summary['quality_score']:>9.1f}%")
    print(f"  Duration:          {summary['duration_sec']:>9.1f}s")
    print(f"  Status:            {'✅ ' + summary['status']}")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Healthcare Claims ETL Pipeline")
    parser.add_argument("--date",    type=str, default=None,       help="Run date YYYY-MM-DD")
    parser.add_argument("--source",  type=str, default="data/raw", help="Source file or directory")
    parser.add_argument("--dry-run", action="store_true",          help="Skip load step")
    args = parser.parse_args()

    run_pipeline(
        source_path=args.source,
        run_date=args.date,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
