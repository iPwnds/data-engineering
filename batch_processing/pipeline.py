from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
log = logging.getLogger("pipeline")


# defines what's valid for one column: whether it's required, its allowed numeric range, and any fixed set of acceptable values
@dataclass(frozen=True)
class ColumnRule:
    mandatory: bool
    min_val: float | None = None
    max_val: float | None = None
    valid_values: frozenset | None = None
    description: str = ""


_TLC = (1, 265)  # NYC TLC zone ID range

COLUMN_RULES: dict[str, ColumnRule] = {
    # Mandatory
    "tpep_pickup_datetime": ColumnRule(
        mandatory=True,
        description="Pickup datetime; non-null",
    ),
    "tpep_dropoff_datetime": ColumnRule(
        mandatory=True,
        description="Dropoff datetime; non-null, must be > pickup",
    ),
    "passenger_count": ColumnRule(
        mandatory=True, min_val=1, max_val=9,
        description="Number of passengers; integer 1–9",
    ),
    "trip_distance": ColumnRule(
        mandatory=True, min_val=0.01, max_val=200.0,
        description="Trip distance in miles; 0.01–200",
    ),
    "PULocationID": ColumnRule(
        mandatory=True, min_val=_TLC[0], max_val=_TLC[1],
        description=f"Pickup TLC zone ID; {_TLC[0]}–{_TLC[1]}",
    ),
    "DOLocationID": ColumnRule(
        mandatory=True, min_val=_TLC[0], max_val=_TLC[1],
        description=f"Dropoff TLC zone ID; {_TLC[0]}–{_TLC[1]}",
    ),
    "payment_type": ColumnRule(
        mandatory=True,
        valid_values=frozenset({0, 1, 2, 3, 4, 5, 6}),
        description="0=unspecified 1=credit 2=cash 3=no-charge 4=dispute 5=unknown 6=voided",
    ),
    "fare_amount": ColumnRule(
        mandatory=True, min_val=0.0,
        description="Metered fare in USD; ≥ 0",
    ),
    "total_amount": ColumnRule(
        mandatory=True, min_val=0.0,
        description="Total charged in USD; ≥ 0",
    ),
    # Non-mandatory
    "tip_amount": ColumnRule(
        mandatory=False, min_val=0.0,
        description="Tip in USD; ≥ 0 when present",
    ),
    "tolls_amount": ColumnRule(
        mandatory=False, min_val=0.0,
        description="Tolls in USD; ≥ 0 when present",
    ),
    "extra": ColumnRule(
        mandatory=False,
        description="Miscellaneous extras; may be negative (corrections)",
    ),
    "Airport_fee": ColumnRule(
        mandatory=False, min_val=0.0,
        description="Airport surcharge in USD; ≥ 0 when present",
    ),
    "congestion_surcharge": ColumnRule(
        mandatory=False, min_val=0.0,
        description="MTA congestion surcharge; ≥ 0 when present",
    ),
    "cbd_congestion_fee": ColumnRule(
        mandatory=False, min_val=0.0,
        description="CBD congestion pricing fee; ≥ 0 when present",
    ),
    "store_and_fwd_flag": ColumnRule(
        mandatory=False,
        valid_values=frozenset({"Y", "N"}),
        description="Trip held in vehicle memory; Y or N, dropped after validation",
    ),
    "RatecodeID": ColumnRule(
        mandatory=False,
        valid_values=frozenset({1, 2, 3, 4, 5, 6}),
        description="Rate code 1–6, dropped after validation",
    ),
}

MANDATORY_COLUMNS     = {c for c, r in COLUMN_RULES.items() if r.mandatory}
NON_MANDATORY_COLUMNS = {c for c, r in COLUMN_RULES.items() if not r.mandatory}

DROPPED_COLUMNS = {"VendorID", "store_and_fwd_flag", "RatecodeID"}


# returned by both validators, fatal=True means a required column is missing and the pipeline must stop immediately
@dataclass
class ValidationResult:
    fatal: bool = False
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.fatal

    def __str__(self) -> str:
        if self.fatal:
            status = "FATAL"
        elif self.errors:
            status = "PASS (data-quality issues found, rows will be dropped)"
        else:
            status = "PASS"
        lines = [status]
        for e in self.errors:
            lines.append(f"  ERROR   {e}")
        for w in self.warnings:
            lines.append(f"  WARNING {w}")
        return "\n".join(lines)


# loads the source parquet file into a DataFrame
class Reader:

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read(self) -> pd.DataFrame:
        # checks the file exists, loads the parquet, and logs the row/column count
        log.info("Reading %s", self.path)
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        df = pd.read_parquet(self.path)
        log.info("Loaded %d rows × %d cols", *df.shape)
        return df


# pre-processing check that runs before any cleaning, flags data quality issues and stops the pipeline on schema errors
class Validator:
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        # walks every column rule and checks for missing columns, nulls, out-of-range values, and invalid enum values
        log.info("Running pre-processing validation …")
        result = ValidationResult()
        n = len(df)

        for col, rule in COLUMN_RULES.items():
            present = col in df.columns

            if not present:
                if rule.mandatory:
                    result.fatal = True
                    result.errors.append(f"[FATAL] Mandatory column '{col}' missing from source")
                else:
                    result.warnings.append(f"Non-mandatory column '{col}' absent from source")
                continue

            series = df[col]
            # mandatory issues are errors (rows dropped), non-mandatory are warnings
            add = result.errors.append if rule.mandatory else result.warnings.append
            tag = "mandatory" if rule.mandatory else "non-mandatory"

            nulls = series.isna().sum()
            if nulls:
                suffix = ", rows will be dropped" if rule.mandatory else ""
                add(f"{col} ({tag}): {nulls:,} null values ({nulls/n:.1%}){suffix}")

            non_null = series.dropna()

            if rule.min_val is not None:
                bad = (non_null < rule.min_val).sum()
                if bad:
                    suffix = ", rows will be dropped" if rule.mandatory else ""
                    add(f"{col} ({tag}): {bad:,} values < minimum {rule.min_val}{suffix}")

            if rule.max_val is not None:
                bad = (non_null > rule.max_val).sum()
                if bad:
                    suffix = ", rows will be dropped" if rule.mandatory else ""
                    add(f"{col} ({tag}): {bad:,} values > maximum {rule.max_val}{suffix}")

            if rule.valid_values is not None:
                check = (
                    non_null.str.upper().str.strip()
                    if series.dtype == object
                    else non_null
                )
                bad = (~check.isin(rule.valid_values)).sum()
                if bad:
                    suffix = ", rows will be dropped" if rule.mandatory else ""
                    add(f"{col} ({tag}): {bad:,} values outside {set(rule.valid_values)}{suffix}")

        if {"tpep_pickup_datetime", "tpep_dropoff_datetime"} <= set(df.columns):
            bad_order = (df["tpep_dropoff_datetime"] <= df["tpep_pickup_datetime"]).sum()
            if bad_order:
                result.errors.append(
                    f"tpep_dropoff_datetime ≤ tpep_pickup_datetime "
                    f"in {bad_order:,} rows, rows will be dropped"
                )

        log.info("Pre-validation result:\n%s", result)
        return result


# removes invalid rows, fills optional nulls with 0, and adds 10 new derived columns
class Processor:
    MAX_TRIP_HOURS    = 6
    MAX_TRIP_DISTANCE = 200.0
    MAX_FARE          = 1_000.0

    _DISTANCE_BINS   = [0,   2,  10, float("inf")]
    _DISTANCE_LABELS = ["Short", "Medium", "Long"]   # Short<2  Medium 2–10  Long>10

    _FARE_BINS   = [0,  20,  50, float("inf")]
    _FARE_LABELS = ["Low", "Medium", "High"]          # Low<20  Medium 20–50  High>50

    _TIME_BINS   = [-1,   6,  12,  18,  24]
    _TIME_LABELS = ["Night", "Morning", "Afternoon", "Evening"]  # 0–6  7–12  13–18  19–23

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # cleans in stages: remove invalid rows -> fill optional nulls with 0 -> add derived columns -> drop unwanted columns
        log.info("Processing %d rows …", len(df))
        df = df.copy()

        # drop rows with nulls in any required field
        before = len(df)
        df = df.dropna(subset=list(MANDATORY_COLUMNS & set(df.columns)))
        log.info("  Dropped %d rows, nulls in mandatory columns", before - len(df))

        # dropoff must come after pickup
        before = len(df)
        df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]
        log.info("  Dropped %d rows, dropoff ≤ pickup", before - len(df))

        # keep only January 2025 trips, a handful of rows bleed in from Dec 2024 / Feb 2025
        before = len(df)
        df = df[
            (df["tpep_pickup_datetime"].dt.year == 2025) &
            (df["tpep_pickup_datetime"].dt.month == 1)
        ]
        log.info("  Dropped %d rows, pickup outside January 2025", before - len(df))

        # passenger count, distance, location IDs, payment type, fare
        before = len(df)
        df = df[
            df["passenger_count"].between(1, 9) &
            df["trip_distance"].between(0.01, self.MAX_TRIP_DISTANCE) &
            df["PULocationID"].between(1, 265) &
            df["DOLocationID"].between(1, 265) &
            df["payment_type"].isin({0, 1, 2, 3, 4, 5, 6}) &
            (df["fare_amount"]  >= 0) &
            (df["total_amount"] >= 0)
        ]
        log.info("  Dropped %d rows, mandatory column domain violations", before - len(df))

        # compute duration, then cut anything unreasonably long or expensive
        df["trip_duration_minutes"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds() / 60
        ).round(2)
        before = len(df)
        df = df[
            df["trip_duration_minutes"].between(1, self.MAX_TRIP_HOURS * 60) &
            (df["fare_amount"]  <= self.MAX_FARE) &
            (df["total_amount"] <= self.MAX_FARE)
        ]
        log.info("  Dropped %d rows, extreme outliers (duration/fare)", before - len(df))

        # optional columns default to 0
        df["congestion_surcharge"] = df["congestion_surcharge"].fillna(0.0)
        df["Airport_fee"]          = df["Airport_fee"].fillna(0.0)
        df["tip_amount"]           = df["tip_amount"].fillna(0.0)
        df["tolls_amount"]         = df["tolls_amount"].fillna(0.0)
        df["extra"]                = df["extra"].fillna(0.0)
        df["cbd_congestion_fee"]   = df["cbd_congestion_fee"].fillna(0.0)

        # derived columns
        df["average_speed_mph"]  = (
            df["trip_distance"] / (df["trip_duration_minutes"] / 60)
        ).round(2)
        df["pickup_year"]        = df["tpep_pickup_datetime"].dt.year
        df["pickup_month"]       = df["tpep_pickup_datetime"].dt.month
        df["pickup_hour"]        = df["tpep_pickup_datetime"].dt.hour
        df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.day_name()
        df["pickup_date"]        = df["tpep_pickup_datetime"].dt.date
        df["revenue_per_mile"]   = (df["total_amount"] / df["trip_distance"]).round(2)

        df["trip_distance_category"] = pd.cut(
            df["trip_distance"],
            bins=self._DISTANCE_BINS,
            labels=self._DISTANCE_LABELS,
            right=False,
            include_lowest=True,
        ).astype(str)

        df["fare_category"] = pd.cut(
            df["fare_amount"],
            bins=self._FARE_BINS,
            labels=self._FARE_LABELS,
            right=False,
            include_lowest=True,
        ).astype(str)

        df["trip_time_of_day"] = pd.cut(
            df["pickup_hour"],
            bins=self._TIME_BINS,
            labels=self._TIME_LABELS,
            right=True,
        ).astype(str)

        df = df.drop(columns=list(DROPPED_COLUMNS & set(df.columns)))

        log.info("Processing complete, %d rows, %d cols", *df.shape)
        return df


_EXPECTED_DERIVED = {
    "trip_duration_minutes", "average_speed_mph",
    "pickup_year", "pickup_month", "pickup_hour", "pickup_day_of_week", "pickup_date",
    "revenue_per_mile", "trip_distance_category", "fare_category", "trip_time_of_day",
}


# post-processing sanity check, verifies the processor did what it was supposed to and no impossible values slipped through
class BackupValidator:
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        # confirms all derived columns were created, all dropped columns are gone, and no nulls remain in mandatory fields
        log.info("Running backup (post-processing) validation …")
        result = ValidationResult()

        for col in _EXPECTED_DERIVED:
            if col not in df.columns:
                result.fatal = True
                result.errors.append(f"[FATAL] Expected derived column '{col}' not found")

        for col in DROPPED_COLUMNS:
            if col in df.columns:
                result.errors.append(f"Column '{col}' should have been dropped but is still present")

        if result.fatal:
            log.info("Backup-validation result:\n%s", result)
            return result

        for col in MANDATORY_COLUMNS:
            if col in df.columns:
                nulls = df[col].isna().sum()
                if nulls:
                    result.errors.append(
                        f"{col} (mandatory): {nulls:,} nulls still present after processing"
                    )

        if (df["trip_duration_minutes"] <= 0).any():
            result.errors.append("trip_duration_minutes ≤ 0 in some rows after processing")

        if (df["average_speed_mph"] <= 0).any():
            result.errors.append("average_speed_mph ≤ 0 in some rows after processing")

        if (df["revenue_per_mile"] < 0).any():
            result.errors.append("revenue_per_mile < 0 in some rows after processing")

        filled_cols = (
            "congestion_surcharge", "Airport_fee",
            "tip_amount", "tolls_amount", "extra", "cbd_congestion_fee",
        )
        for col in filled_cols:
            if col in df.columns and df[col].isna().any():
                result.errors.append(f"{col}: nulls remain after fill")

        unexpected = set(df["trip_distance_category"].unique()) - {"Short", "Medium", "Long"}
        if unexpected:
            result.errors.append(f"trip_distance_category has unexpected labels: {unexpected}")

        unexpected = set(df["fare_category"].unique()) - {"Low", "Medium", "High"}
        if unexpected:
            result.errors.append(f"fare_category has unexpected labels: {unexpected}")

        unexpected = (
            set(df["trip_time_of_day"].unique()) - {"Night", "Morning", "Afternoon", "Evening"}
        )
        if unexpected:
            result.errors.append(f"trip_time_of_day has unexpected labels: {unexpected}")

        if len(df) == 0:
            result.fatal = True
            result.errors.append("[FATAL] DataFrame is empty after processing")
        elif len(df) < 1_000:
            result.warnings.append(f"Very few rows remaining: {len(df):,}")

        log.info("Backup-validation result:\n%s", result)
        return result


# saves the cleaned parquet and a text report locally, then uploads both to Azure Blob Storage
class Writer:
    OUTPUT_FILENAME = "yellow_tripdata_2025-01_processed.parquet"
    REPORT_FILENAME = "pipeline_report.txt"

    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, df: pd.DataFrame, original_rows: int | None = None) -> dict[str, Path | str]:
        # writes cleaned parquet and summary report locally; uploads to Azure if the connection string env var is set
        if original_rows is None:
            original_rows = len(df)
        data_path   = self.output_dir / self.OUTPUT_FILENAME
        report_path = self.output_dir / self.REPORT_FILENAME

        log.info("Writing %d rows locally → %s", len(df), data_path)
        df.to_parquet(data_path, index=False)
        report_path.write_text(self._build_report(df, original_rows))
        log.info("Report written → %s", report_path)

        outputs: dict[str, Path | str] = {
            "local_data":   data_path,
            "local_report": report_path,
        }

        conn_str  = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        container = os.environ.get("AZURE_CONTAINER")

        if conn_str and container:
            azure_url = self._upload_to_azure(data_path, report_path, conn_str, container)
            outputs["azure_data"]   = f"{azure_url}/{self.OUTPUT_FILENAME}"
            outputs["azure_report"] = f"{azure_url}/{self.REPORT_FILENAME}"
        else:
            log.warning(
                "AZURE_STORAGE_CONNECTION_STRING or AZURE_CONTAINER not set, "
                "skipping cloud upload"
            )

        return outputs

    def _upload_to_azure(
        self,
        data_path: Path,
        report_path: Path,
        conn_str: str,
        container: str,
    ) -> str:
        # uploads the data file and report; retries up to 3 times with 2s -> 4s -> 8s delays between attempts
        import time
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise ImportError(
                "azure-storage-blob is not installed. "
                "Run: pip install azure-storage-blob"
            ) from exc

        _MAX_RETRIES = 3
        client   = BlobServiceClient.from_connection_string(conn_str)
        base_url = f"https://{client.account_name}.blob.core.windows.net/{container}"

        for local_path in (data_path, report_path):
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    blob = client.get_blob_client(container=container, blob=local_path.name)
                    with open(local_path, "rb") as fh:
                        blob.upload_blob(fh, overwrite=True)
                    log.info("Uploaded → %s/%s", base_url, local_path.name)
                    break
                except Exception as exc:
                    if attempt == _MAX_RETRIES:
                        raise
                    wait = 2 ** attempt
                    log.warning(
                        "Azure upload attempt %d/%d failed (%s). Retrying in %ds …",
                        attempt, _MAX_RETRIES, exc, wait,
                    )
                    time.sleep(wait)

        return base_url

    def _build_report(self, df: pd.DataFrame, original_rows: int) -> str:
        # builds the plain-text summary: quality score, trip averages, and breakdowns by distance/fare/time of day/payment
        quality_score = len(df) / original_rows * 100 if original_rows > 0 else 0.0
        lines = [
            "=== Pipeline Report ===",
            f"Original rows          : {original_rows:,}",
            f"Rows written           : {len(df):,}",
            f"Rows rejected          : {original_rows - len(df):,}",
            f"Data quality score     : {quality_score:.1f}%",
            f"Date range             : {df['pickup_date'].min()} → {df['pickup_date'].max()}",
            f"Avg trip duration      : {df['trip_duration_minutes'].mean():.1f} min",
            f"Avg trip distance      : {df['trip_distance'].mean():.2f} miles",
            f"Avg speed              : {df['average_speed_mph'].mean():.1f} mph",
            f"Avg fare               : ${df['fare_amount'].mean():.2f}",
            f"Avg total              : ${df['total_amount'].mean():.2f}",
            f"Avg revenue / mile     : ${df['revenue_per_mile'].mean():.2f}",
            "",
            "--- Trip distance categories ---",
        ]
        for cat, cnt in df["trip_distance_category"].value_counts().sort_index().items():
            lines.append(f"  {cat:<8}: {cnt:>10,}")

        lines += ["", "--- Fare categories ---"]
        for cat, cnt in df["fare_category"].value_counts().sort_index().items():
            lines.append(f"  {cat:<8}: {cnt:>10,}")

        lines += ["", "--- Time of day ---"]
        for tod in ("Night", "Morning", "Afternoon", "Evening"):
            cnt = (df["trip_time_of_day"] == tod).sum()
            lines.append(f"  {tod:<10}: {cnt:>10,}")

        lines += ["", "--- Trips by payment type ---"]
        payment_labels = {
            0: "Unspecified", 1: "Credit card", 2: "Cash",
            3: "No charge",   4: "Dispute",     5: "Unknown", 6: "Voided trip",
        }
        for ptype, cnt in df["payment_type"].value_counts().sort_index().items():
            lines.append(f"  {payment_labels.get(ptype, ptype):<15}: {cnt:>10,}")

        lines += ["", "--- Trips by day of week ---"]
        day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        for day, cnt in (
            df["pickup_day_of_week"].value_counts().reindex(day_order).dropna().items()
        ):
            lines.append(f"  {day:<12}: {cnt:>10,}")

        return "\n".join(lines) + "\n"


# convenience wrapper that chains all stages together for standalone runs outside Airflow
class Pipeline:
    def __init__(self, source: str | Path, output_dir: str | Path) -> None:
        self.reader           = Reader(source)
        self.validator        = Validator()
        self.processor        = Processor()
        self.backup_validator = BackupValidator()
        self.writer           = Writer(output_dir)

    def run(self) -> dict[str, Path | str]:
        # runs all pipeline stages in order and returns a dict of output file paths
        df = self.reader.read()
        original_rows = len(df)

        result = self.validator.validate(df)
        if result.fatal:
            raise RuntimeError(f"Pre-validation fatal error:\n{result}")

        df = self.processor.process(df)

        result = self.backup_validator.validate(df)
        if not result.passed:
            raise RuntimeError(f"Backup validation fatal error:\n{result}")

        return self.writer.write(df, original_rows=original_rows)


if __name__ == "__main__":
    _here = Path(__file__).parent
    pipeline = Pipeline(
        source     = _here / "input" / "yellow_tripdata_2025-01.parquet",
        output_dir = _here / "output",
    )
    outputs = pipeline.run()
    print("\nOutputs:")
    for key, path in outputs.items():
        print(f"  {key}: {path}")
