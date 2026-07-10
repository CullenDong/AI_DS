"""
Submit a local .py or .ipynb to SageMaker as a Processing Job.

Mirrors the style of aws-example/jobs/examples/sagemaker_training_job_submit.py
but uses Processing (not Training) because we want to execute arbitrary
notebooks / scripts, not train a model.

Usage:
    python -m jobs.sagemaker_run.run_submit path/to/script.py
    python -m jobs.sagemaker_run.run_submit path/to/notebook.ipynb
    python -m jobs.sagemaker_run.run_submit path/to/notebook.ipynb --instance ml.m5.xlarge

The script/notebook is uploaded to s3://<S3_BUCKET>/<S3_PREFIX>/code/ along with
run_entry.py + requirements.txt (which contains papermill). Outputs land in
s3://<S3_BUCKET>/<S3_PREFIX>/output/<job-name>/.

Prerequisites (run once):
  aws configure                    # access key / secret / region
  # IAM user needs: AmazonSageMakerFullAccess + AmazonS3FullAccess at minimum
  # Plus a SageMaker execution role ARN with s3 + sagemaker permissions.

Edit the CONFIG block below (or export env vars) before running.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from sagemaker.processing import FrameworkProcessor, ProcessingInput, ProcessingOutput
from sagemaker.sklearn.estimator import SKLearn

# ----------------------------------------------------------------------------
# CONFIG — override with env vars or edit here.
# ----------------------------------------------------------------------------
REGION = os.environ.get("AWS_REGION", "us-west-2")
S3_BUCKET = os.environ.get("SAGEMAKER_S3_BUCKET", "bituslabs-team-ai")
S3_PREFIX = os.environ.get("SAGEMAKER_S3_PREFIX", "ai-ds/run-jobs")
SAGEMAKER_ROLE = os.environ.get(
    "SAGEMAKER_ROLE_ARN",
    "arn:aws:iam::338568447110:role/SageMakerExecutionRole",
)
DEFAULT_INSTANCE = os.environ.get("SAGEMAKER_INSTANCE_TYPE", "ml.m5.large")
FRAMEWORK_VERSION = "1.2-1"  # SKLearn image version; provides Python 3.10
# ----------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent


def build_processor(instance_type: str, instance_count: int, job_name: str) -> FrameworkProcessor:
    return FrameworkProcessor(
        estimator_cls=SKLearn,
        framework_version=FRAMEWORK_VERSION,
        role=SAGEMAKER_ROLE,
        instance_type=instance_type,
        instance_count=instance_count,
        base_job_name=job_name,
        sagemaker_session=None,  # use default session (uses AWS_REGION / ~/.aws/config)
    )


def submit(target: Path, instance_type: str, instance_count: int, wait: bool) -> str:
    if not target.exists():
        print(f"target not found: {target}", file=sys.stderr)
        sys.exit(1)
    if target.suffix not in {".py", ".ipynb"}:
        print(f"unsupported file type: {target.suffix}", file=sys.stderr)
        sys.exit(1)

    stamp = time.strftime("%Y%m%d-%H%M%S")
    job_name = f"run-{target.stem[:30].replace('_', '-')}-{stamp}"
    output_s3 = f"s3://{S3_BUCKET}/{S3_PREFIX}/output/{job_name}/"

    processor = build_processor(instance_type, instance_count, job_name)

    # FrameworkProcessor will upload `source_dir` (this folder, containing
    # run_entry.py + requirements.txt) and pip-install the requirements in the
    # container before invoking run_entry.py.
    # We separately pass the target file as a ProcessingInput so run_entry.py
    # can find it at /opt/ml/processing/input/code/<filename>.
    inputs = [
        ProcessingInput(
            source=str(target),
            destination="/opt/ml/processing/input/code",
            input_name="user_target",
        ),
    ]
    outputs = [
        ProcessingOutput(
            source="/opt/ml/processing/output",
            destination=output_s3,
            output_name="result",
        ),
    ]

    print(f"[submit] job_name   = {job_name}")
    print(f"[submit] target     = {target}")
    print(f"[submit] instance   = {instance_type} x{instance_count}")
    print(f"[submit] role       = {SAGEMAKER_ROLE}")
    print(f"[submit] output_s3  = {output_s3}")

    processor.run(
        code="run_entry.py",
        source_dir=str(HERE),
        arguments=["--target", target.name],
        inputs=inputs,
        outputs=outputs,
        wait=wait,
        logs=wait,
    )
    return output_s3


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="run_submit",
        description="Submit a local .py or .ipynb to SageMaker Processing.",
    )
    parser.add_argument("target", type=Path, help="path to a .py or .ipynb file")
    parser.add_argument("--instance", default=DEFAULT_INSTANCE, help="instance type (default: %(default)s)")
    parser.add_argument("--count", type=int, default=1, help="instance count (default: 1)")
    parser.add_argument("--no-wait", action="store_true", help="submit and return instead of streaming logs")
    args = parser.parse_args()

    output_s3 = submit(
        target=args.target.resolve(),
        instance_type=args.instance,
        instance_count=args.count,
        wait=not args.no_wait,
    )
    print(f"[submit] done. outputs at: {output_s3}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
