import json
from pathlib import Path

import boto3


def upload_file(local_path: str | Path, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), bucket, key)


def upload_json(payload: dict, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )