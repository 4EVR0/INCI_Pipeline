from pathlib import Path

import boto3


def upload_file(local_path: str | Path, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), bucket, key)
