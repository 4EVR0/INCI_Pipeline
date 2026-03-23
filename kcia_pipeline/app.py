import csv
import os

from config import get_settings
from extract import extract_all
from transform import transform_to_bronze
from validate import validate
from load_s3 import upload_file


def save_csv(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].__dict__.keys())
        writer.writeheader()
        for r in rows:
            writer.writerow(r.__dict__)


def main():
    settings = get_settings()

    raw_rows, stats = extract_all(settings)
    bronze_rows = transform_to_bronze(raw_rows, settings)

    result = validate(bronze_rows, stats, settings)
    if not result.is_valid:
        raise Exception(result.message)

    local_path = f"output/kcia_{settings.ingest_date}.csv"
    save_csv(bronze_rows, local_path)

    s3_key = f"{settings.s3_prefix}/ingest_date={settings.ingest_date}/kcia.csv"

    upload_file(local_path, settings.s3_bucket, s3_key)

    print("Upload complete:", s3_key)


if __name__ == "__main__":
    main()