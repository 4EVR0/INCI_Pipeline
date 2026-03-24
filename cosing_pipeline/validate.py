import pandas as pd

from cosing_pipeline.models import ExtractionStats, ValidationResult


def validate_bronze(df: pd.DataFrame, stats: ExtractionStats, settings) -> ValidationResult:
    if df.empty:
        return ValidationResult(False, "Collected dataframe is empty")

    if settings.strict_validation and stats.raw_result_count != len(df):
        return ValidationResult(
            False,
            f"Raw result count and transformed row count differ: {stats.raw_result_count} != {len(df)}",
        )

    required_columns = ["inci_name", "item_type", "source", "ingest_date", "batch_id"]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        return ValidationResult(False, f"Missing required columns: {missing}")

    return ValidationResult(True, "OK")
