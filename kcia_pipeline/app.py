import csv
from dataclasses import asdict

from common.metadata import build_kcia_bronze_metadata, write_json
from common.paths import ensure_dir, get_kcia_bronze_paths
from kcia_pipeline.config import get_settings
from kcia_pipeline.extract import extract_all
from kcia_pipeline.load_s3 import upload_file, upload_json
from kcia_pipeline.transform import transform_to_bronze
from kcia_pipeline.validate import validate
from kcia_pipeline.utils.logging_utils import setup_logger

logger = setup_logger()


def save_csv(rows, path):
    if not rows:
        raise ValueError("No rows to save")

    ensure_dir(path.parent)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=asdict(rows[0]).keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def _clear_checkpoint_files(paths) -> None:
    if paths.checkpoint_state_path.exists():
        paths.checkpoint_state_path.unlink()
    if paths.checkpoint_rows_path.exists():
        paths.checkpoint_rows_path.unlink()


def main():
    settings = get_settings()
    paths = get_kcia_bronze_paths(
        batch_month=settings.batch_month,
        s3_prefix=settings.s3_prefix,
    )

    logger.info("Starting KCIA pipeline")
    logger.info("Ingest date: %s", settings.ingest_date)
    logger.info("Batch month: %s", settings.batch_month)
    logger.info("Local CSV path: %s", paths.local_csv_path)
    logger.info("S3 CSV key: %s", paths.s3_csv_key)

    raw_rows, stats = extract_all(
        settings,
        checkpoint_state_path=paths.checkpoint_state_path,
        checkpoint_rows_path=paths.checkpoint_rows_path,
    )
    logger.info("Extracted rows: %s", len(raw_rows))

    bronze_rows = transform_to_bronze(raw_rows, settings)
    logger.info("After transform: %s rows", len(bronze_rows))

    result = validate(bronze_rows, stats, settings)
    if not result.is_valid:
        logger.error(result.message)
        raise Exception(result.message)

    logger.info("Validation passed")

    save_csv(bronze_rows, paths.local_csv_path)
    logger.info("Saved locally: %s", paths.local_csv_path)

    metadata = build_kcia_bronze_metadata(
        batch_month=settings.batch_month,
        ingest_date=settings.ingest_date,
        batch_id=settings.batch_id,
        row_count=len(bronze_rows),
        total_expected=stats.total_expected,
        total_collected=stats.total_collected,
        local_csv_path=str(paths.local_csv_path),
        s3_bucket=settings.s3_bucket,
        s3_csv_key=paths.s3_csv_key,
    )

    write_json(paths.local_metadata_path, metadata)
    logger.info("Saved local metadata: %s", paths.local_metadata_path)

    upload_file(str(paths.local_csv_path), settings.s3_bucket, paths.s3_csv_key)
    logger.info("Uploaded CSV to S3: %s", paths.s3_csv_key)

    upload_json(metadata, settings.s3_bucket, paths.s3_metadata_key)
    logger.info("Uploaded metadata to S3: %s", paths.s3_metadata_key)

    if settings.clear_checkpoint_on_success:
        _clear_checkpoint_files(paths)
        logger.info("Checkpoint files removed after successful completion")

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()