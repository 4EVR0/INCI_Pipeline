import csv
import os


from kcia_pipeline.config import get_settings
from kcia_pipeline.extract import extract_all
from kcia_pipeline.transform import transform_to_bronze
from kcia_pipeline.validate import validate
from kcia_pipeline.load_s3 import upload_file

from kcia_pipeline.utils.logging_utils import setup_logger

logger = setup_logger()


def save_csv(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].__dict__.keys())
        writer.writeheader()
        for r in rows:
            writer.writerow(r.__dict__)


def main():
    settings = get_settings()

    logger.info("Starting KCIA pipeline")

    raw_rows, stats = extract_all(settings)
    logger.info(f"Extracted rows: {len(raw_rows)}")

    bronze_rows = transform_to_bronze(raw_rows, settings)
    logger.info(f"After transform: {len(bronze_rows)} rows")

    result = validate(bronze_rows, stats, settings)
    if not result.is_valid:
        logger.error(result.message)
        raise Exception(result.message)

    logger.info("Validation passed")

    local_path = f"output/kcia_{settings.ingest_date}.csv"
    save_csv(bronze_rows, local_path)
    logger.info(f"Saved locally: {local_path}")

    s3_key = f"{settings.s3_prefix}/ingest_date={settings.ingest_date}/kcia.csv"

    upload_file(local_path, settings.s3_bucket, s3_key)
    logger.info(f"Uploaded to S3: {s3_key}")

    logger.info("Pipeline completed")


if __name__ == "__main__":
    main()