"""
GlowCheck — Apache Airflow DAG
Orchestrates the full pipeline on a daily schedule:
  1. Extract      — pull from all 5 sources
  2. Preprocess   — clean and feature-engineer each data type
  3. EDA          — missingness + cross-source validation
  4. Transform    — join, aggregate, score
  5. Load         — full + incremental + CDC to PostgreSQL/MongoDB
  6. Validate     — run production quality checks
  7. Alert        — notify if any stage fails
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

log = logging.getLogger(__name__)

default_args = {
    "owner":            "subhitcha",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="glowcheck_pipeline",
    default_args=default_args,
    description="GlowCheck daily ETL pipeline — extract, transform, load, validate",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["glowcheck", "etl", "skincare"],
)


def run_script(script_path):
    import subprocess
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    log.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"{script_path} failed:\n{result.stderr}")


def extract_pubmed(**ctx):    run_script("scripts/extract/extract_pubmed.py")
def extract_openfda(**ctx):   run_script("scripts/extract/extract_openfda.py")
def extract_cosing(**ctx):    run_script("scripts/extract/extract_cosing.py")
def extract_obf(**ctx):       run_script("scripts/extract/extract_openbeautyfacts.py")
def preprocess_all(**ctx):
    for s in ["text","images","audio","structured"]:
        run_script(f"scripts/preprocess/preprocess_{s}.py")
def run_eda(**ctx):           run_script("scripts/eda/eda_report.py")
def run_transform(**ctx):     run_script("scripts/etl/transform.py")
def run_load(**ctx):          run_script("scripts/etl/load.py")
def run_cdc(**ctx):           run_script("scripts/etl/cdc.py")
def run_validation(**ctx):    run_script("scripts/production/resilient_pipeline.py")


def on_failure_alert(context):
    log.error(
        "PIPELINE ALERT: Task '%s' in DAG '%s' failed at %s.",
        context["task_instance"].task_id,
        context["dag"].dag_id,
        context["execution_date"]
    )


def make_task(task_id, fn):
    return PythonOperator(
        task_id=task_id,
        python_callable=fn,
        on_failure_callback=on_failure_alert,
        dag=dag,
    )


t_extract_pubmed  = make_task("extract_pubmed",  extract_pubmed)
t_extract_openfda = make_task("extract_openfda", extract_openfda)
t_extract_cosing  = make_task("extract_cosing",  extract_cosing)
t_extract_obf     = make_task("extract_obf",     extract_obf)
t_preprocess      = make_task("preprocess_all",  preprocess_all)
t_eda             = make_task("run_eda",          run_eda)
t_transform       = make_task("run_transform",   run_transform)
t_load            = make_task("run_load",         run_load)
t_cdc             = make_task("run_cdc",          run_cdc)
t_validate        = make_task("run_validation",  run_validation)

# Pipeline order:
# extract_* (parallel) → preprocess → eda → transform → load → cdc → validate
[t_extract_pubmed, t_extract_openfda,
 t_extract_cosing, t_extract_obf] >> t_preprocess

t_preprocess >> t_eda >> t_transform >> t_load >> t_cdc >> t_validate