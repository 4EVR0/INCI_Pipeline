from common.metadata import write_json
from cosing_pipeline.config import get_settings
from cosing_pipeline.extract.extract import extract_all
from cosing_pipeline.load_s3 import upload_file, upload_json
from cosing_pipeline.transform.transform import transform_to_bronze
from cosing_pipeline.utils.logging_utils import setup_logger
from cosing_pipeline.validate import validate_bronze

logger = setup_logger()


def build_cosing_bronze_metadata(
    *,
    batch_month: str,
    ingest_date: str,
    batch_id: str,
    row_count: int,
    stats,
    local_csv_path: str,
    local_parquet_path: str,
    s3_bucket: str,
    s3_csv_key: str,
    s3_parquet_key: str,
):
    return {
        "source": "cosing",
        "layer": "bronze",
        "batch_month": batch_month,
        "ingest_date": ingest_date,
        "batch_id": batch_id,
        "row_count": row_count,
        "extraction_stats": {
            "final_query_count": stats.final_query_count,
            "oversized_query_count": stats.oversized_query_count,
            "raw_page_count": stats.raw_page_count,
            "raw_result_count": stats.raw_result_count,
        },
        "artifacts": {
            "local_csv_path": local_csv_path,
            "local_parquet_path": local_parquet_path,
            "s3_bucket": s3_bucket,
            "s3_csv_key": s3_csv_key,
            "s3_parquet_key": s3_parquet_key,
        },
        "status": "success",
    }


def main():
    settings = get_settings()
    logger.info("Starting CosIng Bronze pipeline")
    logger.info("Ingest date: %s", settings.ingest_date)
    logger.info("Batch month: %s", settings.batch_month)

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

    local_csv = settings.output_dir / "cosing_bronze.csv"
    local_parquet = settings.output_dir / "cosing_bronze.parquet"
    local_metadata = settings.output_dir / "metadata.json"

    df.to_csv(local_csv, index=False, encoding="utf-8-sig")
    df.to_parquet(local_parquet, index=False)

    logger.info("Saved locally: %s", local_csv)
    logger.info("Saved locally: %s", local_parquet)

    s3_csv_key = f"{settings.s3_prefix}/batch={settings.batch_month}/cosing_bronze.csv"
    s3_parquet_key = f"{settings.s3_prefix}/batch={settings.batch_month}/cosing_bronze.parquet"
    s3_metadata_key = f"{settings.s3_prefix}/batch={settings.batch_month}/metadata.json"

    metadata = build_cosing_bronze_metadata(
        batch_month=settings.batch_month,
        ingest_date=settings.ingest_date,
        batch_id=settings.batch_id,
        row_count=len(df),
        stats=stats,
        local_csv_path=str(local_csv),
        local_parquet_path=str(local_parquet),
        s3_bucket=settings.s3_bucket,
        s3_csv_key=s3_csv_key,
        s3_parquet_key=s3_parquet_key,
    )

    write_json(local_metadata, metadata)
    logger.info("Saved local metadata: %s", local_metadata)

    upload_file(local_csv, settings.s3_bucket, s3_csv_key)
    upload_file(local_parquet, settings.s3_bucket, s3_parquet_key)
    upload_json(metadata, settings.s3_bucket, s3_metadata_key)

    logger.info("Uploaded to S3: s3://%s/%s", settings.s3_bucket, s3_csv_key)
    logger.info("Uploaded to S3: s3://%s/%s", settings.s3_bucket, s3_parquet_key)
    logger.info("Uploaded to S3: s3://%s/%s", settings.s3_bucket, s3_metadata_key)
    logger.info("CosIng Bronze pipeline completed")


if __name__ == "__main__":
    main()