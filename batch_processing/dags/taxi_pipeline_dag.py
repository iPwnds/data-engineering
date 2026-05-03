from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# resolve() follows the symlink so sys.path points to the real project folder when Airflow loads this from ~/airflow/dags/
_BATCH_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BATCH_DIR))

from pipeline import (  # noqa: E402
    BackupValidator,
    Processor,
    Reader,
    Validator,
    Writer,
)

log = logging.getLogger(__name__)

SOURCE_PATH = str(_BATCH_DIR / "input" / "yellow_tripdata_2025-01.parquet")
OUTPUT_DIR = str(_BATCH_DIR / "output")

# intermediate files in tmp so we don't pass large DataFrames through XCom
_TMP_RAW = "/tmp/taxi_raw.parquet"
_TMP_PROC = "/tmp/taxi_processed.parquet"


def task_read(**context) -> str:
    # reads the parquet file and stages it in tmp; also pushes the original row count to XCom so the writer can compute the quality score
    reader = Reader(SOURCE_PATH)
    df = reader.read()
    df.to_parquet(_TMP_RAW, index=False)
    # store original row count so the writer can calculate the quality score later
    context["task_instance"].xcom_push(key="original_rows", value=len(df))
    log.info("Read %d rows; saved to %s", len(df), _TMP_RAW)
    return _TMP_RAW


def task_validate(**context) -> str:
    # pulls the staged parquet from tmp, runs the pre-processing validator, and halts if a mandatory column is missing
    import pandas as pd

    path = context["task_instance"].xcom_pull(task_ids="read")
    df = pd.read_parquet(path)

    validator = Validator()
    result = validator.validate(df)

    # fatal means a required column is missing, no point continuing
    if result.fatal:
        raise RuntimeError(f"Pre-validation fatal:\n{result}")

    log.info(
        "Pre-validation passed (errors are row-level and will be handled by processor)."
    )
    return path


def task_process(**context) -> str:
    # runs the processor on the validated data and writes the cleaned result back to tmp
    import pandas as pd

    path = context["task_instance"].xcom_pull(task_ids="validate")
    df = pd.read_parquet(path)

    processor = Processor()
    df = processor.process(df)
    df.to_parquet(_TMP_PROC, index=False)
    log.info("Processed %d rows; saved to %s", len(df), _TMP_PROC)
    return _TMP_PROC


def task_backup_validate(**context) -> str:
    # confirms the processor added all expected derived columns and no bad values survived cleaning
    import pandas as pd

    path = context["task_instance"].xcom_pull(task_ids="process")
    df = pd.read_parquet(path)

    bv = BackupValidator()
    result = bv.validate(df)

    # sanity check that all derived columns exist and no impossible values slipped through
    if not result.passed:
        raise RuntimeError(f"Backup validation failed:\n{result}")

    log.info("Backup validation passed.")
    return path


def task_write(**context) -> dict[str, str]:
    # writes the cleaned parquet and report locally, then uploads both to Azure; returns a dict of output paths
    import pandas as pd

    path = context["task_instance"].xcom_pull(task_ids="backup_validate")
    df = pd.read_parquet(path)
    # pull original row count from the read task to calculate data quality score
    original_rows = context["task_instance"].xcom_pull(
        task_ids="read", key="original_rows"
    )

    writer = Writer(OUTPUT_DIR)
    outputs = writer.write(df, original_rows=original_rows)
    log.info("Write complete: %s", outputs)
    return {k: str(v) for k, v in outputs.items()}


with DAG(
    dag_id="nyc_taxi_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 8 4 5 *",  # defence: 4 May 2026 08:00
    catchup=False,  # don't backfill missed runs
    tags=["taxi", "data-engineering"],
) as dag:
    read = PythonOperator(
        task_id="read",
        python_callable=task_read,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
    )

    process = PythonOperator(
        task_id="process",
        python_callable=task_process,
    )

    backup_validate = PythonOperator(
        task_id="backup_validate",
        python_callable=task_backup_validate,
    )

    write = PythonOperator(
        task_id="write",
        python_callable=task_write,
    )

    read >> validate >> process >> backup_validate >> write
