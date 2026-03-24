from pathlib import Path

from cosing_pipeline.config import get_settings
from cosing_pipeline.extract.extract import extract_all
from cosing_pipeline.load_s3 import upload_file
from cosing_pipeline.transform.transform import transform_to_bronze
from cosing_pipeline.utils.logging_utils import setup_logger
from cosing_pipeline.validate import validate_bronze

logger = setup_logger()


def main():
    settings = get_settings()
    logger.info("Starting CosIng Bronze pipeline")

    raw_pages, stats = extract_all(settings=settings)
    logger.info(
        "Extracted raw pages=%s raw results=%s queries=%s",
        stats.raw_page_count,
        stats.raw_result_count,
        stats.final_query_count,
    )

    df = transform_to_bronze(raw_pages, settings)
    logger.info("After transform: %s rows", len(df))

    validation = validate_bronze(df, stats, settings)
    if not validation.is_valid:
        logger.error(validation.message)
        raise RuntimeError(validation.message)

    logger.info("Validation passed")

    local_csv = settings.output_dir / f"cosing_bronze_{settings.ingest_date}.csv"
    local_parquet = settings.output_dir / f"cosing_bronze_{settings.ingest_date}.parquet"

    df.to_csv(local_csv, index=False, encoding="utf-8-sig")
    df.to_parquet(local_parquet, index=False)

    logger.info("Saved locally: %s", local_csv)
    logger.info("Saved locally: %s", local_parquet)

    s3_csv_key = f"{settings.s3_prefix}/ingest_date={settings.ingest_date}/cosing.csv"
    s3_parquet_key = f"{settings.s3_prefix}/ingest_date={settings.ingest_date}/cosing.parquet"

    upload_file(local_csv, settings.s3_bucket, s3_csv_key)
    upload_file(local_parquet, settings.s3_bucket, s3_parquet_key)

    logger.info("Uploaded to S3: s3://%s/%s", settings.s3_bucket, s3_csv_key)
    logger.info("Uploaded to S3: s3://%s/%s", settings.s3_bucket, s3_parquet_key)
    logger.info("CosIng Bronze pipeline completed")


if __name__ == "__main__":
    main()
