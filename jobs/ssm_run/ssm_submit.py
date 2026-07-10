"""
Trigger a SageMaker Classic Notebook Instance (on-demand) from your local Mac
to execute a notebook in place (on its EBS), and upload results to S3.

Flow:
    Local Mac
      → boto3 start DataScience instance (if stopped)
      → wait until InService
      → resolve its underlying EC2 instance id
      → SSM SendCommand: cd into notebook dir, papermill target.ipynb,
        `aws s3 cp` result to S3
      → poll SSM until command completes
      → print stdout/stderr tails
      → (optional) stop instance

Prereqs (one-time setup on the DataScience notebook instance):
  1. Attach AWS managed policy `AmazonSSMManagedInstanceCore` to the
     notebook instance's IAM role (so SSM Agent can register).
  2. After the policy is attached, start the instance once — SSM Agent
     (preinstalled on recent SageMaker AMIs) will register it.
  3. Attach an S3 write policy (or a targeted inline one) to the same
     role so the instance can `aws s3 cp` results to bucket.

Prereqs on your Mac:
  * `aws configure` done (user with sagemaker + ssm + ec2 describe perms)
  * boto3 installed (in requirements.txt)

Usage:
    python -m jobs.ssm_run.ssm_submit 3.21M1ProModel/M1ProScore.ipynb
    python -m jobs.ssm_run.ssm_submit foo.ipynb --conda-env python3 --stop-after
    python -m jobs.ssm_run.ssm_submit foo.ipynb --no-wait  # fire and forget
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import PurePosixPath

import boto3
from botocore.exceptions import ClientError

INSTANCE_NAME = os.environ.get("SM_INSTANCE_NAME", "DataScience")
REGION = os.environ.get("AWS_REGION", "us-west-2")
S3_BUCKET = os.environ.get("SAGEMAKER_S3_BUCKET", "bituslabs-team-ai")
S3_PREFIX = os.environ.get("SAGEMAKER_S3_PREFIX", "ai-ds/run-jobs")
REMOTE_NB_ROOT = "/home/ec2-user/SageMaker"  # Classic Notebook Instance EBS mount
DEFAULT_CONDA_ENV = "python3"

# SSM RunCommand timeout for the whole shell invocation (seconds). Notebooks
# that run longer than this will be killed. Bump per your workload.
DEFAULT_SSM_TIMEOUT = 6 * 3600


def main() -> int:
    p = argparse.ArgumentParser(prog="ssm_submit")
    p.add_argument(
        "target",
        help=f"notebook path relative to {REMOTE_NB_ROOT} (e.g. '3.21M1ProModel/M1ProScore.ipynb')",
    )
    p.add_argument("--conda-env", default=DEFAULT_CONDA_ENV, help="conda env to activate (default: %(default)s)")
    p.add_argument("--timeout", type=int, default=DEFAULT_SSM_TIMEOUT, help="SSM command timeout in seconds")
    p.add_argument("--stop-after", action="store_true", help="stop the instance after the job completes")
    p.add_argument("--no-wait", action="store_true", help="send command and return immediately")
    args = p.parse_args()

    sess = boto3.Session(region_name=REGION)
    sm = sess.client("sagemaker")
    ec2 = sess.client("ec2")
    ssm = sess.client("ssm")

    # 1. Ensure instance is InService.
    status = _describe_status(sm, INSTANCE_NAME)
    print(f"[{INSTANCE_NAME}] current status: {status}")
    if status == "Stopped":
        print(f"[{INSTANCE_NAME}] starting...")
        sm.start_notebook_instance(NotebookInstanceName=INSTANCE_NAME)
        status = _wait_for_status(sm, INSTANCE_NAME, "InService")
    elif status in ("Pending", "Updating", "Stopping"):
        print(f"[{INSTANCE_NAME}] waiting for transition...")
        status = _wait_for_status(sm, INSTANCE_NAME, "InService")
    elif status != "InService":
        print(f"[{INSTANCE_NAME}] unexpected status: {status}", file=sys.stderr)
        return 2

    # 2. Resolve the underlying EC2 instance id (SageMaker tags the instance).
    ec2_id = _resolve_ec2_id(ec2, INSTANCE_NAME)
    print(f"[{INSTANCE_NAME}] ec2 instance id: {ec2_id}")

    # 3. Verify SSM sees the instance.
    _verify_ssm_registered(ssm, ec2_id)

    # 4. Build the shell script and send it.
    stamp = time.strftime("%Y%m%d-%H%M%S")
    target_rel = PurePosixPath(args.target)
    job_name = f"{target_rel.stem[:40].replace(' ', '_')}-{stamp}"
    output_s3 = f"s3://{S3_BUCKET}/{S3_PREFIX}/output/{job_name}/"
    script = _build_shell(
        target_rel=str(target_rel),
        conda_env=args.conda_env,
        output_s3=output_s3,
        job_name=job_name,
    )

    print(f"[submit] target    = {REMOTE_NB_ROOT}/{target_rel}")
    print(f"[submit] conda_env = {args.conda_env}")
    print(f"[submit] output_s3 = {output_s3}")

    send = ssm.send_command(
        InstanceIds=[ec2_id],
        DocumentName="AWS-RunShellScript",
        Comment=f"AI_DS run {job_name}",
        TimeoutSeconds=args.timeout,
        Parameters={
            "commands": [script],
            "executionTimeout": [str(args.timeout)],
        },
    )
    command_id = send["Command"]["CommandId"]
    print(f"[submit] ssm command id: {command_id}")

    if args.no_wait:
        print("[submit] --no-wait set; exiting.")
        return 0

    # 5. Poll the invocation until terminal.
    rc = _poll_invocation(ssm, command_id, ec2_id)
    if args.stop_after:
        print(f"[{INSTANCE_NAME}] stopping...")
        sm.stop_notebook_instance(NotebookInstanceName=INSTANCE_NAME)
    return rc


def _describe_status(sm, name: str) -> str:
    try:
        resp = sm.describe_notebook_instance(NotebookInstanceName=name)
        return resp["NotebookInstanceStatus"]
    except ClientError as e:
        raise SystemExit(f"describe_notebook_instance failed: {e}")


def _wait_for_status(sm, name: str, target: str, interval: int = 15, timeout: int = 900) -> str:
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        s = _describe_status(sm, name)
        if s != last:
            print(f"  [{name}] {s}")
            last = s
        if s == target:
            return s
        if s == "Failed":
            raise SystemExit(f"{name} entered Failed state")
        time.sleep(interval)
    raise SystemExit(f"timed out waiting for {name} -> {target}")


def _resolve_ec2_id(ec2, nb_name: str) -> str:
    # SageMaker tags the underlying EC2 with aws:sagemaker:notebook-instance-name
    resp = ec2.describe_instances(
        Filters=[
            {"Name": "tag:aws:sagemaker:notebook-instance-name", "Values": [nb_name]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )
    ids = [i["InstanceId"] for r in resp["Reservations"] for i in r["Instances"]]
    if not ids:
        raise SystemExit(
            f"no running EC2 instance tagged 'aws:sagemaker:notebook-instance-name={nb_name}'. "
            "Is the notebook instance actually InService?"
        )
    if len(ids) > 1:
        print(f"warning: multiple matches, picking first: {ids}", file=sys.stderr)
    return ids[0]


def _verify_ssm_registered(ssm, ec2_id: str) -> None:
    resp = ssm.describe_instance_information(Filters=[{"Key": "InstanceIds", "Values": [ec2_id]}])
    info = resp.get("InstanceInformationList", [])
    if not info:
        raise SystemExit(
            f"SSM does not see {ec2_id}. Did you attach AmazonSSMManagedInstanceCore to the "
            "notebook instance's IAM role and restart the instance?"
        )
    if info[0].get("PingStatus") != "Online":
        raise SystemExit(f"SSM Agent on {ec2_id} is not Online: {info[0]}")


def _build_shell(*, target_rel: str, conda_env: str, output_s3: str, job_name: str) -> str:
    # Runs on the notebook instance. Writes papermill result + logs next to the
    # notebook in /tmp, then uploads them to S3. Assumes notebook_dir already
    # contains all the data files the notebook references (since it's the
    # instance's own EBS).
    nb_path = f"{REMOTE_NB_ROOT}/{target_rel}"
    out_local_dir = f"/tmp/ai_ds_runs/{job_name}"
    out_nb = f"{out_local_dir}/$(basename '{target_rel}' .ipynb).executed.ipynb"
    return f"""set -eo pipefail
NB_DIR="$(dirname '{nb_path}')"
mkdir -p "{out_local_dir}"
cd "$NB_DIR"

# Activate conda env (SageMaker Notebook Instance ships anaconda at /home/ec2-user/anaconda3)
source /home/ec2-user/anaconda3/etc/profile.d/conda.sh
conda activate {conda_env}

# Ensure papermill is available in this env; install quietly if missing.
python -c "import papermill" 2>/dev/null || pip install --quiet papermill ipykernel

echo "[remote] executing {nb_path}"
papermill "{nb_path}" "{out_nb}" \\
    --kernel python3 \\
    --log-output \\
    --stdout-file "{out_local_dir}/stdout.log" \\
    --stderr-file "{out_local_dir}/stderr.log"
RC=$?
echo "[remote] papermill exit code: $RC"

aws s3 cp --recursive "{out_local_dir}/" "{output_s3}"
echo "[remote] uploaded to {output_s3}"
exit $RC
"""


def _poll_invocation(ssm, command_id: str, ec2_id: str, interval: int = 15) -> int:
    terminal = {"Success", "Cancelled", "TimedOut", "Failed"}
    last = None
    while True:
        try:
            inv = ssm.get_command_invocation(CommandId=command_id, InstanceId=ec2_id)
        except ClientError as e:
            if e.response["Error"]["Code"] == "InvocationDoesNotExist":
                time.sleep(interval)
                continue
            raise
        status = inv["Status"]
        if status != last:
            print(f"[ssm] {status}")
            last = status
        if status in terminal:
            _print_tail("stdout", inv.get("StandardOutputContent", ""))
            _print_tail("stderr", inv.get("StandardErrorContent", ""))
            return 0 if status == "Success" else 1
        time.sleep(interval)


def _print_tail(label: str, text: str, lines: int = 40) -> None:
    if not text:
        return
    tail = "\n".join(text.splitlines()[-lines:])
    print(f"\n--- {label} (last {lines} lines) ---\n{tail}")


if __name__ == "__main__":
    raise SystemExit(main())
