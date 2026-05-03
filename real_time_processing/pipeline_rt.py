from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
log = logging.getLogger("pipeline_rt")

SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}

MANDATORY_COLUMNS = {
    "product_id", "product_name", "category",
    "discounted_price", "actual_price", "discount_percentage", "rating",
}
NON_MANDATORY_COLUMNS = {
    "rating_count", "about_product", "user_id", "user_name",
    "review_id", "review_title", "review_content", "img_link", "product_link",
}

_PRODUCT_ID_RE = re.compile(r"^B[A-Z0-9]{9}$")

_PRICE_TIER_BINS   = [0, 500, 5_000, float("inf")]
_PRICE_TIER_LABELS = ["Budget", "Mid-range", "Premium"]   # <₹500  ₹500–5k  >₹5k

# upper bound is 5.01 rather than 5.0 so that a rating of exactly 5.0 falls in "High"
_RATING_BINS   = [0.0, 3.5, 4.2, 5.01]
_RATING_LABELS = ["Low", "Medium", "High"]                # <3.5  3.5–4.2  >4.2


@dataclass
class ValidationResult:
    fatal: bool = False
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.fatal

    def __str__(self) -> str:
        status = "FATAL" if self.fatal else (
            "PASS (data-quality issues — rows will be dropped)" if self.errors else "PASS"
        )
        lines = [status]
        for e in self.errors:
            lines.append(f"  ERROR   {e}")
        for w in self.warnings:
            lines.append(f"  WARNING {w}")
        return "\n".join(lines)


class Reader:

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def read(self) -> pd.DataFrame:
        log.info("Reading %s", self.path)
        if not self.path.exists():
            raise FileNotFoundError(f"File not found: {self.path}")
        ext = self.path.suffix.lower()
        if ext not in SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file type '{ext}'. Supported: {SUPPORTED_EXTENSIONS}")
        if self.path.stat().st_size == 0:
            raise ValueError(f"File is empty (0 bytes): {self.path.name}")
        df = self._read_csv() if ext == ".csv" else self._read_xlsx()
        if df.empty:
            raise ValueError(f"File contains no data rows: {self.path.name}")
        log.info("Loaded %d rows x %d cols", *df.shape)
        return df

    def _read_csv(self) -> pd.DataFrame:
        # try common encodings in order — Amazon export files sometimes arrive as latin-1
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                df = pd.read_csv(self.path, encoding=enc, dtype=str)
                log.info("Decoded with encoding: %s", enc)
                return df
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Could not decode {self.path.name} — may be binary or corrupt")

    def _read_xlsx(self) -> pd.DataFrame:
        try:
            df = pd.read_excel(self.path, dtype=str)
            log.info("Decoded as .xlsx")
            return df
        except Exception as exc:
            raise ValueError(f"Could not read Excel file: {exc}") from exc


class Validator:

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        log.info("Running pre-processing validation ...")
        result = ValidationResult()
        n = len(df)

        # mandatory columns must all be present
        for col in MANDATORY_COLUMNS:
            if col not in df.columns:
                result.fatal = True
                result.errors.append(f"[FATAL] Mandatory column '{col}' missing")
        if result.fatal:
            log.info("Pre-validation result:\n%s", result)
            return result

        # nulls in mandatory columns
        for col in MANDATORY_COLUMNS:
            nulls = df[col].isna().sum()
            if nulls:
                result.errors.append(
                    f"{col}: {nulls:,} nulls ({nulls/n:.1%}) — rows will be dropped")

        # product_id should follow Amazon ASIN format
        bad = (~df["product_id"].dropna().astype(str).str.match(_PRODUCT_ID_RE)).sum()
        if bad:
            result.errors.append(
                f"product_id: {bad:,} values not matching ASIN format — rows will be dropped")

        dupes = df["product_id"].dropna().duplicated().sum()
        if dupes:
            result.errors.append(
                f"product_id: {dupes:,} duplicates — first occurrence kept")

        # prices are stored as strings like ₹1,099 — check they're parseable
        for col in ("discounted_price", "actual_price"):
            parsed = pd.to_numeric(
                df[col].str.replace(r"[₹,]", "", regex=True), errors="coerce")
            unparseable = parsed.isna().sum() - df[col].isna().sum()
            if unparseable:
                result.errors.append(
                    f"{col}: {unparseable:,} unparseable values — rows will be dropped")

        # rating should be 1.0-5.0
        parsed = pd.to_numeric(df["rating"], errors="coerce")
        unparseable = parsed.isna().sum() - df["rating"].isna().sum()
        if unparseable:
            result.errors.append(
                f"rating: {unparseable:,} unparseable values — rows will be dropped")
        out_of_range = ((parsed < 1) | (parsed > 5)).sum()
        if out_of_range:
            result.errors.append(
                f"rating: {out_of_range:,} values outside 1–5 — rows will be dropped")

        # discount_percentage stored as "64%" — check parseability
        parsed = pd.to_numeric(
            df["discount_percentage"].str.replace("%", "", regex=False), errors="coerce")
        unparseable = parsed.isna().sum() - df["discount_percentage"].isna().sum()
        if unparseable:
            result.errors.append(
                f"discount_percentage: {unparseable:,} unparseable values — rows will be dropped")

        # rating_count is optional but warn if nulls present
        if "rating_count" in df.columns:
            nulls = df["rating_count"].isna().sum()
            if nulls:
                result.warnings.append(
                    f"rating_count: {nulls:,} nulls — will be filled with 0")

        log.info("Pre-validation result:\n%s", result)
        return result


class Processor:

    def __init__(self) -> None:
        self.rejection_log: pd.DataFrame = pd.DataFrame()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        log.info("Processing %d rows ...", len(df))
        df = df.copy()
        parts: list[pd.DataFrame] = []

        # deduplicate on product_id, keep first occurrence
        dup = df["product_id"].duplicated(keep="first")
        if dup.any():
            r = df[dup].copy()
            r["rejection_reason"] = "Duplicate product_id"
            r["rejected_at_step"] = 1
            parts.append(r)
        df = df[~dup]
        log.info("  Step 1 — removed %d duplicate rows", dup.sum())

        # drop rows missing required fields
        mand = list(MANDATORY_COLUMNS & set(df.columns))
        null_any = df[mand].isna().any(axis=1)
        if null_any.any():
            r = df[null_any].copy()
            first_null = df[mand][null_any].apply(
                lambda row: next((c for c in mand if pd.isna(row[c])), "unknown"), axis=1)
            r["rejection_reason"] = "Null in mandatory column: " + first_null
            r["rejected_at_step"] = 2
            parts.append(r)
        df = df[~null_any]
        log.info("  Step 2 — dropped %d rows (mandatory nulls)", null_any.sum())

        # strip currency symbols and parse all numeric columns
        for col in ("discounted_price", "actual_price"):
            df[col] = pd.to_numeric(
                df[col].str.replace(r"[₹,]", "", regex=True), errors="coerce")
        df["discount_percentage"] = pd.to_numeric(
            df["discount_percentage"].str.replace("%", "", regex=False), errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        if "rating_count" in df.columns:
            df["rating_count"] = pd.to_numeric(
                df["rating_count"].str.replace(",", "", regex=False), errors="coerce"
            ).fillna(0).astype(int)

        # drop rows with invalid values after parsing — track the first failing condition
        # per row so the rejection log shows a useful reason rather than just "invalid"
        conds = [
            df["discounted_price"].isna() | (df["discounted_price"] <= 0),
            df["actual_price"].isna() | (df["actual_price"] <= 0),
            df["discount_percentage"].isna(),
            df["rating"].isna() | (df["rating"] < 1) | (df["rating"] > 5),
            ~df["product_id"].astype(str).str.match(_PRODUCT_ID_RE),
        ]
        msgs = [
            "discounted_price invalid or unparseable",
            "actual_price invalid or unparseable",
            "discount_percentage unparseable",
            "rating out of range 1–5 or unparseable",
            "product_id does not match Amazon ASIN format",
        ]
        bad = pd.Series(False, index=df.index)
        why = pd.Series("", index=df.index)
        for cond, msg in zip(conds, msgs):
            new = cond & ~bad
            bad = bad | cond
            why[new] = msg
        if bad.any():
            r = df[bad].copy()
            r["rejection_reason"] = why[bad]
            r["rejected_at_step"] = 3
            parts.append(r)
        df = df[~bad]
        log.info("  Step 3 — dropped %d rows (invalid values)", bad.sum())

        # derived columns
        df["top_category"]    = df["category"].str.split("|").str[0]
        df["discount_amount"] = (df["actual_price"] - df["discounted_price"]).round(2)
        df["price_tier"] = pd.cut(
            df["discounted_price"],
            bins=_PRICE_TIER_BINS,
            labels=_PRICE_TIER_LABELS,
            right=False, include_lowest=True,
        ).astype(str)
        df["rating_band"] = pd.cut(
            df["rating"],
            bins=_RATING_BINS,
            labels=_RATING_LABELS,
            right=True, include_lowest=True,
        ).astype(str)

        if parts:
            self.rejection_log = pd.concat(parts, ignore_index=True)
        else:
            self.rejection_log = pd.DataFrame()

        log.info("Processing complete — %d rows kept, %d rows rejected",
                 len(df), len(self.rejection_log))
        return df


_EXPECTED_DERIVED = {"top_category", "discount_amount", "price_tier", "rating_band"}


class BackupValidator:

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        log.info("Running backup (post-processing) validation ...")
        result = ValidationResult()

        for col in _EXPECTED_DERIVED:
            if col not in df.columns:
                result.fatal = True
                result.errors.append(f"[FATAL] Expected derived column '{col}' not found")

        if result.fatal:
            log.info("Backup-validation result:\n%s", result)
            return result

        if df.empty:
            result.warnings.append("0 valid rows remain — output file will be empty")
            log.info("Backup-validation result:\n%s", result)
            return result

        if df["product_id"].duplicated().any():
            result.errors.append("product_id duplicates still present after dedup")

        for col in MANDATORY_COLUMNS:
            if col in df.columns and df[col].isna().any():
                result.errors.append(f"{col}: nulls remain after processing")

        if (df["discounted_price"] <= 0).any():
            result.errors.append("discounted_price has non-positive values")
        if (df["actual_price"] <= 0).any():
            result.errors.append("actual_price has non-positive values")
        if (df["discount_amount"] < 0).any():
            result.errors.append("discount_amount is negative (discounted > actual price)")
        if ((df["rating"] < 1) | (df["rating"] > 5)).any():
            result.errors.append("rating out of range 1–5")

        unexpected = set(df["price_tier"].unique()) - {"Budget", "Mid-range", "Premium"}
        if unexpected:
            result.errors.append(f"price_tier has unexpected labels: {unexpected}")
        unexpected = set(df["rating_band"].unique()) - {"Low", "Medium", "High"}
        if unexpected:
            result.errors.append(f"rating_band has unexpected labels: {unexpected}")

        if len(df) < 10:
            result.warnings.append(f"Very few rows remaining: {len(df):,}")

        log.info("Backup-validation result:\n%s", result)
        return result


_AZURE_MAX_RETRIES = 3


class Writer:

    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, df: pd.DataFrame, source_name: str,
              rejection_log: pd.DataFrame | None = None,
              original_rows: int | None = None) -> dict[str, Path | str]:
        if rejection_log is None:
            rejection_log = pd.DataFrame()
        if original_rows is None:
            original_rows = len(df)

        stem           = Path(source_name).stem
        data_path      = self.output_dir / f"{stem}_processed.csv"
        report_path    = self.output_dir / f"{stem}_report.txt"
        rejection_path = self.output_dir / f"{stem}_rejected.csv"
        quality_score  = (len(df) / original_rows * 100) if original_rows > 0 else 0.0

        log.info("Writing %d rows -> %s", len(df), data_path)
        df.to_csv(data_path, index=False)
        report_path.write_text(
            self._build_report(df, source_name, original_rows, quality_score))
        log.info("Report -> %s", report_path)

        outputs: dict[str, Path | str] = {
            "local_data": data_path, "local_report": report_path}

        if not rejection_log.empty:
            rejection_log.to_csv(rejection_path, index=False)
            log.info("Rejection log -> %s  (%d rows)", rejection_path, len(rejection_log))
            outputs["local_rejected"] = rejection_path

        conn_str  = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        container = os.environ.get("AZURE_CONTAINER")
        if conn_str and container:
            targets = [data_path, report_path]
            if not rejection_log.empty:
                targets.append(rejection_path)
            try:
                base = self._upload_to_azure(targets, conn_str, container)
                outputs["azure_data"]   = f"{base}/{data_path.name}"
                outputs["azure_report"] = f"{base}/{report_path.name}"
                if not rejection_log.empty:
                    outputs["azure_rejected"] = f"{base}/{rejection_path.name}"
            except Exception as exc:
                log.warning("Azure upload failed after %d retries: %s",
                            _AZURE_MAX_RETRIES, exc)
        else:
            log.warning("AZURE_STORAGE_CONNECTION_STRING or AZURE_CONTAINER not set "
                        "— skipping cloud upload")
        return outputs

    def _upload_to_azure(self, paths: list[Path], conn_str: str, container: str) -> str:
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise ImportError("Run: pip install azure-storage-blob") from exc
        client   = BlobServiceClient.from_connection_string(conn_str)
        base_url = f"https://{client.account_name}.blob.core.windows.net/{container}"
        for p in paths:
            for attempt in range(1, _AZURE_MAX_RETRIES + 1):
                try:
                    blob = client.get_blob_client(container=container, blob=p.name)
                    with open(p, "rb") as fh:
                        blob.upload_blob(fh, overwrite=True)
                    log.info("Uploaded -> %s/%s", base_url, p.name)
                    break
                except Exception as exc:
                    if attempt == _AZURE_MAX_RETRIES:
                        raise
                    wait = 2 ** attempt
                    log.warning("Upload attempt %d/%d failed (%s). Retrying in %ds ...",
                                attempt, _AZURE_MAX_RETRIES, exc, wait)
                    time.sleep(wait)
        return base_url

    def _build_report(self, df: pd.DataFrame, source_name: str,
                      original_rows: int, quality_score: float) -> str:
        rejected = original_rows - len(df)
        lines = [
            f"=== Pipeline Report — {source_name} ===",
            f"Source file          : {source_name}",
            f"Original rows        : {original_rows:,}",
            f"Rows written         : {len(df):,}",
            f"Rows rejected        : {rejected:,}",
            f"Data quality score   : {quality_score:.1f}%",
        ]
        if df.empty:
            lines += ["", "No valid rows remain after cleaning."]
            return "\n".join(lines) + "\n"
        lines += [
            f"Avg rating           : {df['rating'].mean():.2f}",
            f"Avg discount         : {df['discount_percentage'].mean():.1f}%",
            f"Avg discount amount  : ₹{df['discount_amount'].mean():.2f}",
            f"Avg discounted price : ₹{df['discounted_price'].mean():.2f}",
            "", "--- Products by category ---",
        ]
        for cat, cnt in df["top_category"].value_counts().items():
            avg_r = df.loc[df["top_category"] == cat, "rating"].mean()
            lines.append(f"  {cat:<25}: {cnt:>5,} products   avg rating {avg_r:.2f}")
        lines += ["", "--- Price tiers ---"]
        for tier in ("Budget", "Mid-range", "Premium"):
            cnt = (df["price_tier"] == tier).sum()
            if cnt:
                avg_p = df.loc[df["price_tier"] == tier, "discounted_price"].mean()
                lines.append(f"  {tier:<12}: {cnt:>5,}   avg ₹{avg_p:.0f}")
        lines += ["", "--- Rating bands ---"]
        for band in ("Low", "Medium", "High"):
            cnt = (df["rating_band"] == band).sum()
            lines.append(f"  {band:<8}: {cnt:>5,}")
        return "\n".join(lines) + "\n"


class Pipeline:
    def __init__(self, output_dir: str | Path = "output") -> None:
        self.validator        = Validator()
        self.processor        = Processor()
        self.backup_validator = BackupValidator()
        self.writer           = Writer(output_dir)

    def run(self, file_path: str | Path) -> dict[str, Path | str]:
        file_path     = Path(file_path)
        df            = Reader(file_path).read()
        original_rows = len(df)

        result = self.validator.validate(df)
        if result.fatal:
            raise RuntimeError(f"Pre-validation fatal error:\n{result}")

        df = self.processor.process(df)

        result = self.backup_validator.validate(df)
        if not result.passed:
            raise RuntimeError(f"Backup validation fatal error:\n{result}")

        return self.writer.write(
            df,
            source_name   = file_path.name,
            rejection_log = self.processor.rejection_log,
            original_rows = original_rows,
        )


if __name__ == "__main__":
    import sys
    src      = sys.argv[1] if len(sys.argv) > 1 else "input/amazon.csv"
    pipeline = Pipeline(output_dir="output")
    outputs  = pipeline.run(src)
    print("\nOutputs:")
    for key, path in outputs.items():
        print(f"  {key}: {path}")
