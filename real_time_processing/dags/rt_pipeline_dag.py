from __future__ import annotations

import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

_DAGS_DIR = Path(__file__).parent
_RT_DIR = _DAGS_DIR.parent
_PROJECT_ROOT = _RT_DIR.parent
sys.path.insert(0, str(_RT_DIR))

from pipeline_rt import (  # noqa: E402
    BackupValidator,
    Processor,
    Reader,
    Validator,
    Writer,
)

log = logging.getLogger(__name__)

INPUT_DIR = str(_RT_DIR / "input")
OUTPUT_DIR = str(_RT_DIR / "output")
PROCESSED_DIR = str(_RT_DIR / "processed")

# XCom key used to pass the active file path between tasks
_XCOM_FILE_KEY = "csv_file_path"
_SUPPORTED_EXTS = {".csv", ".xlsx"}
_REJECTED_SUBDIR = "rejected"


def sense_csv_file(**context) -> bool:
    # called every 30 seconds by the PythonSensor; returns True when a valid file is found in input, False to keep waiting
    # scans input for the next valid file; empties and unsupported types go to rejected/
    rejected_dir = Path(INPUT_DIR) / _REJECTED_SUBDIR
    rejected_dir.mkdir(exist_ok=True)

    # iterate files at the top level only, skip the rejected subfolder
    all_files = sorted(
        f
        for f in Path(INPUT_DIR).iterdir()
        if f.is_file() and f.parent == Path(INPUT_DIR)
    )

    for f in all_files:
        ext = f.suffix.lower()

        if f.stat().st_size == 0:
            dest = rejected_dir / f.name
            shutil.move(str(f), str(dest))
            (rejected_dir / f"{f.stem}_reason.txt").write_text(
                f"REJECTED: file is empty (0 bytes), {f.name}\n"
            )
            log.warning("Moved empty file to rejected/: %s", f.name)
            continue

        if ext not in _SUPPORTED_EXTS:
            dest = rejected_dir / f.name
            shutil.move(str(f), str(dest))
            (rejected_dir / f"{f.stem}_reason.txt").write_text(
                f"REJECTED: unsupported file type '{ext}', {f.name}\n"
                f"Supported types: {_SUPPORTED_EXTS}\n"
            )
            log.warning("Moved unsupported file to rejected/: %s", f.name)
            continue

        # first valid file wins, push its path so downstream tasks know what to process
        context["task_instance"].xcom_push(key=_XCOM_FILE_KEY, value=str(f))
        log.info("Detected supported file: %s", f)
        return True

    return False


def _pull_path(context, from_task: str) -> str:
    # helper so every task can fetch the active file path from XCom in one line
    return context["task_instance"].xcom_pull(task_ids=from_task, key=_XCOM_FILE_KEY)


def task_read(**context) -> str:
    # loads the detected file into a DataFrame and stages it as a temp parquet for the next task
    path = _pull_path(context, "sense_file")
    reader = Reader(path)
    df = reader.read()
    # make sure the output directory exists before writing, Writer creates it later but we need it now
    (_RT_DIR / "output").mkdir(parents=True, exist_ok=True)
    # stage raw data as parquet so pandas can reload it in the next task
    tmp = str(_RT_DIR / "output" / ".tmp_raw.parquet")
    df.to_parquet(tmp, index=False)
    context["task_instance"].xcom_push(key=_XCOM_FILE_KEY, value=tmp)
    log.info("Read %d rows; staged to %s", len(df), tmp)
    return tmp


def task_validate(**context) -> str:
    # runs the pre-processing validator and raises an exception if any mandatory column is completely absent
    import pandas as pd

    tmp = _pull_path(context, "read")
    df = pd.read_parquet(tmp)
    result = Validator().validate(df)
    # fatal = a mandatory column is missing, no point continuing
    if result.fatal:
        raise RuntimeError(f"Pre-validation fatal:\n{result}")
    log.info("Pre-validation passed.")
    context["task_instance"].xcom_push(key=_XCOM_FILE_KEY, value=tmp)
    return tmp


def task_process(**context) -> str:
    # cleans the data, builds the rejection log, and stages both as temp files; pushes the original row count to XCom for the writer
    import pandas as pd

    tmp = _pull_path(context, "validate")
    df = pd.read_parquet(tmp)
    original_rows = len(df)
    proc = Processor()
    df = proc.process(df)
    out = str(_RT_DIR / "output" / ".tmp_processed.parquet")
    df.to_parquet(out, index=False)
    # store original count and rejection log path so the writer task can use them
    context["task_instance"].xcom_push(key=_XCOM_FILE_KEY, value=out)
    context["task_instance"].xcom_push(key="original_rows", value=original_rows)
    if not proc.rejection_log.empty:
        rej_out = str(_RT_DIR / "output" / ".tmp_rejected.parquet")
        proc.rejection_log.to_parquet(rej_out, index=False)
        context["task_instance"].xcom_push(key="rejected_path", value=rej_out)
    log.info("Processed %d rows; staged to %s", len(df), out)
    return out


def task_backup_validate(**context) -> str:
    # runs the post-processing sanity check and raises an exception if any derived column is missing or values are out of range
    import pandas as pd

    tmp = _pull_path(context, "process")
    df = pd.read_parquet(tmp)
    result = BackupValidator().validate(df)
    # confirms derived columns exist and no impossible values survived processing
    if not result.passed:
        raise RuntimeError(f"Backup validation failed:\n{result}")
    log.info("Backup validation passed.")
    context["task_instance"].xcom_push(key=_XCOM_FILE_KEY, value=tmp)
    return tmp


def task_write(**context) -> dict[str, str]:
    # writes the cleaned CSV, report, and rejection log; uploads all of them to Azure; returns a dict of output paths
    import pandas as pd

    tmp = _pull_path(context, "backup_validate")
    df = pd.read_parquet(tmp)
    src_path = context["task_instance"].xcom_pull(
        task_ids="sense_file", key=_XCOM_FILE_KEY
    )
    original_rows = context["task_instance"].xcom_pull(
        task_ids="process", key="original_rows"
    )
    rej_path = context["task_instance"].xcom_pull(
        task_ids="process", key="rejected_path"
    )
    # rej_path is only set when there were rejected rows, so default to empty DataFrame
    rejection_log = pd.read_parquet(rej_path) if rej_path else pd.DataFrame()
    outputs = Writer(OUTPUT_DIR).write(
        df,
        Path(src_path).name,
        rejection_log=rejection_log,
        original_rows=original_rows,
    )
    log.info("Write complete: %s", outputs)
    return {k: str(v) for k, v in outputs.items()}


def task_archive(**context) -> str:
    # moves the source file to processed so the sensor doesn't pick it up again on the next run
    src_path = context["task_instance"].xcom_pull(
        task_ids="sense_file", key=_XCOM_FILE_KEY
    )
    dest = Path(PROCESSED_DIR) / Path(src_path).name
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
    shutil.move(src_path, dest)
    log.info("Archived %s → %s", src_path, dest)
    return str(dest)


with DAG(
    dag_id="rt_ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="* * * * *",  # run every minute; the sensor handles the 30 s polling
    catchup=False,
    max_active_runs=1,  # one file at a time, next file waits for this run to finish
    tags=["ecommerce", "real-time", "data-engineering"],
) as dag:
    sense_file = PythonSensor(
        task_id="sense_file",
        python_callable=sense_csv_file,
        poke_interval=30,  # check every 30 seconds
        timeout=3_600,  # give up after 1 hour
        mode="poke",
    )

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

    archive = PythonOperator(
        task_id="archive",
        python_callable=task_archive,
    )

    sense_file >> read >> validate >> process >> backup_validate >> write >> archive
