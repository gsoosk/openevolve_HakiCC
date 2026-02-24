"""
Python evaluator for the CC (Concurrency Control) benchmark.

This evaluator wraps the Java benchmark by:
  - Running the run_evaluator.sh script with the provided Java implementation
  - Parsing the JSON output from the Java evaluator
  - Returning an EvaluationResult compatible with OpenEvolve
"""

import os
import sys
import json
import subprocess
import tempfile
import traceback
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Union

# Pre-known transaction type for throughput scoring
# Valid values: "tpcc" or "auction"
# This affects which benchmark scenarios get priority weighting in the combined_score
# Read from environment variable PRE_KNOWN_TXN_TYPE, default to "tpcc"
PRE_KNOWN_TXN_TYPE = os.environ.get("PRE_KNOWN_TXN_TYPE", "tpcc")

# Try to import from openevolve, fall back to local definition for standalone use
try:
    from openevolve.evaluation_result import EvaluationResult
except ImportError:
    @dataclass
    class EvaluationResult:
        """
        Result of program evaluation containing both metrics and optional artifacts.
        Local fallback when openevolve is not installed.
        """
        metrics: Dict[str, float]
        artifacts: Dict[str, Union[str, bytes, Any]] = field(default_factory=dict)

        @classmethod
        def from_dict(cls, metrics: Dict[str, float]) -> "EvaluationResult":
            return cls(metrics=metrics)

        def to_dict(self) -> Dict[str, float]:
            return self.metrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def evaluate(program_path: str) -> EvaluationResult:
    """
    Entry point used by OpenEvolve to evaluate a concurrency control implementation.

    Args:
        program_path: Path to the Java file containing the CC implementation.

    Returns:
        EvaluationResult with metrics from the Java benchmark including:
          - correctness: fraction of scenarios that are serializable and stall-free
          - throughput_score: normalized throughput score
          - combined_score: weighted combination of correctness and throughput
          - pre_known_transactions_under_high_contention: throughput score under high contention
          - pre_known_transactions_normal_scenario: throughput score for normal scenario (8 warehouses)
    """
    try:
        # Convert to absolute path for use with the script
        program_path = os.path.abspath(program_path)
        
        # Validate input file exists
        if not os.path.isfile(program_path):
            raise FileNotFoundError(f"Program file not found: {program_path}")

        # Get the benchmark directory (where run_evaluator.sh is located)
        evaluator_dir = os.path.dirname(os.path.abspath(__file__))
        benchmark_dir = os.path.join(evaluator_dir, "benchmark")
        run_script = os.path.join(benchmark_dir, "run_evaluator.sh")

        if not os.path.isfile(run_script):
            raise FileNotFoundError(f"run_evaluator.sh not found at: {run_script}")

        # Create a temporary file for the output JSON
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, dir=benchmark_dir
        ) as tmp_output:
            output_file = tmp_output.name

        try:
            # Run the Java evaluator
            logger.info(f"Running Java evaluator for: {program_path}")
            logger.info(f"Output file: {output_file}")
            logger.info(f"Pre-known transaction type: {PRE_KNOWN_TXN_TYPE}")

            result = subprocess.run(
                [
                    "bash", run_script, 
                    program_path, output_file, 
                    "--pre-known-txn", PRE_KNOWN_TXN_TYPE
                ],
                cwd=benchmark_dir,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
            )

            # Log stdout/stderr for debugging
            if result.stdout:
                logger.debug(f"Java evaluator stdout:\n{result.stdout}")
            if result.stderr:
                logger.warning(f"Java evaluator stderr:\n{result.stderr}")

            # Check for execution errors
            if result.returncode != 0:
                error_msg = f"Java evaluator failed with code {result.returncode}"
                if result.stderr:
                    error_msg += f"\nStderr: {result.stderr}"
                if result.stdout:
                    error_msg += f"\nStdout: {result.stdout}"
                raise RuntimeError(error_msg)

            # Parse the output JSON
            if not os.path.isfile(output_file):
                raise FileNotFoundError(
                    f"Output file not created: {output_file}"
                )

            with open(output_file, "r") as f:
                java_result = json.load(f)

            # Extract metrics from Java result
            metrics = java_result.get("metrics", {})
            artifacts = java_result.get("artifacts", {})

            # Ensure required metrics exist with defaults
            # Match the Java Evaluator's output metric names
            final_metrics = {
                "correctness": float(metrics.get("correctness", 0.0)),
                "throughput_score": float(metrics.get("throughput_score", 0.0)),
                "combined_score": float(metrics.get("combined_score", 0.0)),
                "pre_known_transactions_under_high_contention": float(
                    metrics.get("pre_known_transactions_under_high_contention", 0.0)
                ),
                "pre_known_transactions_normal_scenario": float(
                    metrics.get("pre_known_transactions_normal_scenario", 0.0)
                ),
            }

            # Convert artifacts to strings for EvaluationResult compatibility
            final_artifacts: Dict[str, Any] = {}
            if "scenario_summaries" in artifacts:
                final_artifacts["scenario_summaries"] = artifacts["scenario_summaries"]
            if "tpcc_avg_throughputs" in artifacts:
                final_artifacts["tpcc_avg_throughputs"] = artifacts["tpcc_avg_throughputs"]

            # Include any error information from Java evaluator
            if "error" in java_result:
                final_artifacts["java_error"] = java_result["error"]

            logger.info(f"Evaluation complete. Metrics: {final_metrics}")
            logger.info(f"Artifacts: {final_artifacts}")

            return EvaluationResult(metrics=final_metrics, artifacts=final_artifacts)

        finally:
            # Clean up temporary output file
            if os.path.exists(output_file):
                try:
                    os.remove(output_file)
                except OSError:
                    pass

    except subprocess.TimeoutExpired:
        logger.error("Java evaluator timed out after 600 seconds")
        return EvaluationResult(
            metrics={
                "correctness": 0.0,
                "throughput_score": 0.0,
                "combined_score": 0.0,
                "pre_known_transactions_under_high_contention": 0.0,
                "pre_known_transactions_normal_scenario": 0.0,
            },
            artifacts={
                "error_type": "TimeoutError",
                "error_message": "Java evaluator timed out after 600 seconds",
            },
        )

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Java evaluator output: {e}")
        return EvaluationResult(
            metrics={
                "correctness": 0.0,
                "throughput_score": 0.0,
                "combined_score": 0.0,
                "pre_known_transactions_under_high_contention": 0.0,
                "pre_known_transactions_normal_scenario": 0.0,
            },
            artifacts={
                "error_type": "JSONDecodeError",
                "error_message": str(e),
                "full_traceback": traceback.format_exc(),
            },
        )

    except Exception as e:
        logger.error(f"Evaluation failed: {e}")
        return EvaluationResult(
            metrics={
                "correctness": 0.0,
                "throughput_score": 0.0,
                "combined_score": 0.0,
                "pre_known_transactions_under_high_contention": 0.0,
                "pre_known_transactions_normal_scenario": 0.0,
            },
            artifacts={
                "error_type": type(e).__name__,
                "error_message": str(e),
                "full_traceback": traceback.format_exc(),
            },
        )


if __name__ == "__main__":
    # Allow running from command line for testing
    if len(sys.argv) < 2:
        # Default to InitialProgram.java in benchmark directory
        here = os.path.dirname(os.path.abspath(__file__))
        program_path = os.path.join(here, "benchmark", "InitialProgram.java")
    else:
        program_path = sys.argv[1]

    print(f"Evaluating: {program_path}")
    result = evaluate(program_path)
    print("\nMetrics:")
    for key, value in result.metrics.items():
        print(f"  {key}: {value}")
    if result.artifacts:
        print("\nArtifacts:")
        for key in result.artifacts:
            print(f"  {key}: {result.artifacts[key]}")

