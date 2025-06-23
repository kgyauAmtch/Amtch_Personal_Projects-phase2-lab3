from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import boto3
import datetime
import csv
import io

# ── Config ─────────────────────────────────────────────────────────────
BUCKET_NAME     = "lab3-bucket"
RAW_PREFIX      = "raw/streams/"
ARCHIVE_PREFIX  = "archive/streams/"
GLUE_CLEAN_JOB  = "Transformation_job"
GLUE_KPI_JOB    = "dailykpis"
AWS_REGION      = "eu-north-1"
GLUE_ROLE_NAME  = "lab3-gluerole"

EXPECTED_COLUMNS = {
    "user_id",
    "track_id",
    "listen_time"
}

# ── Helper functions ───────────────────────────────────────────────────
def get_latest_s3_file(bucket, prefix):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = resp.get("Contents", [])
    if not files:
        raise ValueError("No files found in S3 prefix")
    return max(files, key=lambda x: x["LastModified"])["Key"]

def file_already_archived(file_name: str) -> bool:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=ARCHIVE_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(f"/{file_name}"):
                return True
    return False

def validate_columns_func(ti, **_):
    key = ti.xcom_pull(task_ids="detect_latest_file", key="stream_file_key")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    first_line = obj["Body"].readline().decode("utf-8")
    reader = csv.reader(io.StringIO(first_line))
    header = set(next(reader))
    missing = EXPECTED_COLUMNS - header
    extras = header - EXPECTED_COLUMNS
    if missing or extras:
        raise ValueError(f"Schema mismatch - missing={missing}, extras={extras}")

def push_latest_file_path(ti, **_):
    key = get_latest_s3_file(BUCKET_NAME, RAW_PREFIX)
    ti.xcom_push(key="stream_file_key", value=key)
    ti.xcom_push(key="stream_file_path", value=f"s3://{BUCKET_NAME}/{key}")

def branch_on_processed(ti, **_):
    key = ti.xcom_pull(task_ids="detect_latest_file", key="stream_file_key")
    file_name = key.split("/")[-1]
    return "end_stream" if file_already_archived(file_name) else "run_clean_glue_job"

def archive_stream_file(**kwargs):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    key = kwargs["ti"].xcom_pull(task_ids="detect_latest_file", key="stream_file_key")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    archive_key = f"{ARCHIVE_PREFIX}{today}/{key.split('/')[-1]}"
    s3.copy_object(Bucket=BUCKET_NAME, CopySource={"Bucket": BUCKET_NAME, "Key": key}, Key=archive_key)
    s3.delete_object(Bucket=BUCKET_NAME, Key=key)

# ── DAG definition ─────────────────────────────────────────────────────
default_args = {"owner": "KOG", "retries": 2, "depends_on_past": False}

with DAG(
    dag_id="glue_stream_etl",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["glue", "etl", "kpis", "dynamodb"],
) as dag:

    wait_for_stream_file = S3KeySensor(
        task_id="wait_for_stream_file",
        bucket_name=BUCKET_NAME,
        bucket_key=f"{RAW_PREFIX}*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=60 * 60,   # 1 hour
        poke_interval=60,  # check every 1 min
        mode="reschedule",
        soft_fail=True
    )

    detect_latest_file = PythonOperator(
        task_id="detect_latest_file",
        python_callable=push_latest_file_path,
    )

    validate_columns = PythonOperator(
        task_id="validate_columns",
        python_callable=validate_columns_func,
    )

    branch_if_processed = BranchPythonOperator(
        task_id="branch_if_processed",
        python_callable=branch_on_processed,
    )

    run_clean_glue_job = GlueJobOperator(
        task_id="run_clean_glue_job",
        job_name=GLUE_CLEAN_JOB,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        iam_role_name=GLUE_ROLE_NAME,
        wait_for_completion=True,
        script_args={
            "--JOB_NAME": GLUE_CLEAN_JOB,
            "--INPUT_FILE_PATH": "{{ ti.xcom_pull(task_ids='detect_latest_file', key='stream_file_path') }}",
        },
    )

    run_kpi_glue_job = GlueJobOperator(
        task_id="run_kpi_glue_job",
        job_name=GLUE_KPI_JOB,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        iam_role_name=GLUE_ROLE_NAME,
        wait_for_completion=True,
        script_args={"--JOB_NAME": GLUE_KPI_JOB},
    )

    archive_stream_data = PythonOperator(
        task_id="archive_stream_data",
        python_callable=archive_stream_file,
    )

    end_stream = EmptyOperator(task_id="end_stream")

    # ── DAG structure ───────────────────────────────────────────────────
    wait_for_stream_file >> detect_latest_file >> validate_columns >> branch_if_processed
    branch_if_processed >> end_stream
    branch_if_processed >> run_clean_glue_job >> run_kpi_glue_job >> archive_stream_data >> end_stream
