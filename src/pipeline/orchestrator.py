"""
pipeline/orchestrator.py
-------------------------
End-to-end pipeline orchestrator for the Healthcare Data Platform.
Runs ingestion, Bronze→Silver, Silver→Gold, and data quality checks
in sequence, recording timing and results for each stage.

Usage
-----
    python orchestrator.py                    # runs full pipeline
    python orchestrator.py --stage ingest
    python orchestrator.py --stage bronze_silver
    python orchestrator.py --stage silver_gold
    python orchestrator.py --stage quality
    python orchestrator.py --stage all
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

# Resolve repository root (parent of the pipeline/ directory)
_REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{line}</cyan> | {message}"
    ),
)

_LOG_DIR = _REPO_ROOT / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(
    _LOG_DIR / "orchestrator.log",
    level="DEBUG",
    rotation="10 MB",
    retention="14 days",
    compression="zip",
)


# ---------------------------------------------------------------------------
# Orchestrator class
# ---------------------------------------------------------------------------

class HealthcarePipeline:
    """
    Coordinates the four pipeline stages:
    1. Ingestion          (raw CSV → bronze Parquet)
    2. Bronze → Silver    (cleaning & enrichment)
    3. Silver → Gold      (aggregated analytics tables)
    4. Data Quality       (checks on silver layer)
    """

    def __init__(self, base_path: Path, local_mode: bool = True) -> None:
        self.base_path = base_path
        self.local_mode = local_mode

        self.raw_path = base_path / "data" / "raw"
        self.bronze_path = base_path / "data" / "bronze"
        self.silver_path = base_path / "data" / "silver"
        self.gold_path = base_path / "data" / "gold"
        self.reports_path = base_path / "pipeline" / "reports"

        self.reports_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"HealthcarePipeline initialised | base_path={base_path} | "
            f"local_mode={local_mode}"
        )

    # ------------------------------------------------------------------
    # Stage runners
    # ------------------------------------------------------------------

    def run_ingestion(self) -> Dict[str, Any]:
        """Stage 1: ingest raw CSVs into the Bronze layer."""
        logger.info("--- Stage 1: Ingestion (raw → bronze) ---")
        # Import here so the orchestrator can run a subset of stages
        # without importing all pipeline dependencies upfront.
        sys.path.insert(0, str(self.base_path / "src" / "ingestion"))
        from ingest import DataIngestionPipeline  # type: ignore[import]
        from config import get_settings  # type: ignore[import]

        settings = get_settings()
        pipeline = DataIngestionPipeline(settings=settings, local_mode=self.local_mode)
        return pipeline.run()

    def run_bronze_to_silver(self) -> Dict[str, Any]:
        """Stage 2: transform Bronze Parquet into Silver."""
        logger.info("--- Stage 2: Bronze → Silver ---")
        sys.path.insert(0, str(self.base_path / "src" / "processing"))
        from bronze_to_silver import SilverTransformer  # type: ignore[import]

        transformer = SilverTransformer()
        return transformer.run(self.bronze_path, self.silver_path)

    def run_silver_to_gold(self) -> Dict[str, Any]:
        """Stage 3: build Gold-layer analytics tables from Silver."""
        logger.info("--- Stage 3: Silver → Gold ---")
        sys.path.insert(0, str(self.base_path / "src" / "processing"))
        from silver_to_gold import GoldTransformer  # type: ignore[import]

        transformer = GoldTransformer()
        return transformer.run(self.silver_path, self.gold_path)

    def run_quality_checks(self) -> Dict[str, Any]:
        """Stage 4: run data quality checks on the Silver layer."""
        logger.info("--- Stage 4: Data Quality Checks ---")
        sys.path.insert(0, str(self.base_path / "src" / "processing"))
        from data_quality import DataQualityChecker  # type: ignore[import]

        checker = DataQualityChecker()
        report = checker.generate_report(self.silver_path)
        checker.save_report(report, self.reports_path)
        return report

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Execute all four stages in sequence.

        Returns
        -------
        dict
            Full pipeline report with per-stage results and elapsed times.
        """
        pipeline_start = time.perf_counter()
        run_timestamp = datetime.utcnow().isoformat()

        logger.info("=" * 60)
        logger.info(" Healthcare Data Platform — Full Pipeline Run")
        logger.info(f" Started at: {run_timestamp}")
        logger.info("=" * 60)

        stages = [
            ("ingestion", self.run_ingestion),
            ("bronze_to_silver", self.run_bronze_to_silver),
            ("silver_to_gold", self.run_silver_to_gold),
            ("quality_checks", self.run_quality_checks),
        ]

        stage_results: Dict[str, Any] = {}
        overall_status = "SUCCESS"

        for stage_name, stage_fn in stages:
            stage_start = time.perf_counter()
            logger.info(f"Starting stage: {stage_name}")
            try:
                result = stage_fn()
                elapsed = round(time.perf_counter() - stage_start, 2)
                stage_results[stage_name] = {
                    "status": "success",
                    "elapsed_seconds": elapsed,
                    "result": result,
                }
                logger.success(f"Stage '{stage_name}' completed in {elapsed}s.")
            except Exception as exc:  # noqa: BLE001
                elapsed = round(time.perf_counter() - stage_start, 2)
                logger.error(f"Stage '{stage_name}' FAILED after {elapsed}s: {exc}")
                stage_results[stage_name] = {
                    "status": "error",
                    "elapsed_seconds": elapsed,
                    "error": str(exc),
                }
                overall_status = "FAILED"
                # Continue running subsequent stages even if one fails
                # (so we get as much output as possible)

        total_elapsed = round(time.perf_counter() - pipeline_start, 2)

        full_report: Dict[str, Any] = {
            "pipeline_run_timestamp": run_timestamp,
            "total_elapsed_seconds": total_elapsed,
            "overall_status": overall_status,
            "stages": stage_results,
        }

        logger.info("=" * 60)
        logger.info(
            f" Pipeline finished | status={overall_status} | "
            f"total_time={total_elapsed}s"
        )
        logger.info("=" * 60)

        return full_report

    # ------------------------------------------------------------------
    # Report persistence
    # ------------------------------------------------------------------

    def save_pipeline_report(self, report: Dict[str, Any]) -> Path:
        """
        Save the full pipeline report as a JSON file under pipeline/reports/.

        Parameters
        ----------
        report:
            Report dict returned by :meth:`run_full_pipeline`.

        Returns
        -------
        Path
            Full path of the written JSON file.
        """
        self.reports_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_file = self.reports_path / f"pipeline_report_{timestamp}.json"

        with open(report_file, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)

        logger.success(f"Pipeline report saved to {report_file}")
        return report_file


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Healthcare Data Platform pipeline orchestrator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python orchestrator.py --stage all\n"
            "  python orchestrator.py --stage ingest\n"
            "  python orchestrator.py --stage bronze_silver\n"
            "  python orchestrator.py --stage silver_gold\n"
            "  python orchestrator.py --stage quality\n"
        ),
    )
    parser.add_argument(
        "--stage",
        choices=["ingest", "bronze_silver", "silver_gold", "quality", "all"],
        default="all",
        help="Pipeline stage to execute (default: all).",
    )
    parser.add_argument(
        "--base-path",
        type=Path,
        default=_REPO_ROOT,
        help=f"Repository root path (default: {_REPO_ROOT}).",
    )
    parser.add_argument(
        "--local-mode",
        action="store_true",
        default=True,
        help="Run in local-mode (write to local filesystem instead of ADLS).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    orchestrator = HealthcarePipeline(
        base_path=args.base_path,
        local_mode=args.local_mode,
    )

    stage_map = {
        "ingest": orchestrator.run_ingestion,
        "bronze_silver": orchestrator.run_bronze_to_silver,
        "silver_gold": orchestrator.run_silver_to_gold,
        "quality": orchestrator.run_quality_checks,
    }

    if args.stage == "all":
        report = orchestrator.run_full_pipeline()
        report_file = orchestrator.save_pipeline_report(report)
    else:
        stage_fn = stage_map[args.stage]
        start = time.perf_counter()
        result = stage_fn()
        elapsed = round(time.perf_counter() - start, 2)
        report = {
            "stage": args.stage,
            "elapsed_seconds": elapsed,
            "result": result,
        }
        report_file = orchestrator.save_pipeline_report(report)

    print("\n" + "=" * 60)
    print("PIPELINE REPORT")
    print("=" * 60)
    print(json.dumps(report, indent=2, default=str))
    print(f"\nFull report saved to: {report_file}")
