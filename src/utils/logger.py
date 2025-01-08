"""
logger.py
Structured logging utility for the Healthcare ETL Pipeline.
Outputs JSON-formatted logs for easy ingestion by log aggregators
(Azure Monitor, Splunk, ELK stack).
"""

import logging
import json
import sys
from datetime import datetime
from pathlib import Path


class PipelineLogger:
    """
    Structured logger that outputs both human-readable console logs
    and JSON-formatted file logs for observability tooling.
    """

    def __init__(self, name: str, log_dir: str = "logs"):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        if self.logger.handlers:
            self.logger.handlers.clear()

        # Console handler — human readable
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        self.logger.addHandler(console)

        # File handler — JSON structured
        log_file = self.log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(file_handler)

    def info(self, message: str, **kwargs):
        self.logger.info(message, extra={"extra_fields": kwargs})

    def warning(self, message: str, **kwargs):
        self.logger.warning(message, extra={"extra_fields": kwargs})

    def error(self, message: str, **kwargs):
        self.logger.error(message, extra={"extra_fields": kwargs})

    def debug(self, message: str, **kwargs):
        self.logger.debug(message, extra={"extra_fields": kwargs})

    def pipeline_start(self, run_date: str, source: str):
        self.info("Pipeline started", run_date=run_date, source=source, event="PIPELINE_START")

    def pipeline_end(self, records_processed: int, duration_seconds: float, status: str):
        self.info(
            f"Pipeline completed in {duration_seconds:.1f}s — {records_processed:,} records",
            records_processed=records_processed,
            duration_seconds=duration_seconds,
            status=status,
            event="PIPELINE_END"
        )

    def stage_complete(self, stage: str, records_in: int, records_out: int, duration_seconds: float):
        self.info(
            f"[{stage}] {records_in:,} in → {records_out:,} out ({duration_seconds:.2f}s)",
            stage=stage,
            records_in=records_in,
            records_out=records_out,
            duration_seconds=duration_seconds,
            event="STAGE_COMPLETE"
        )


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }
        extra = getattr(record, "extra_fields", {})
        log_data.update(extra)
        return json.dumps(log_data)


def get_logger(name: str) -> PipelineLogger:
    return PipelineLogger(name)
