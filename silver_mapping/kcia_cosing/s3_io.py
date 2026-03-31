from __future__ import annotations

import json
import mimetypes
from pathlib import Path
from typing import Any

import boto3


def get_s3_client():
    return boto3.client("s3")


def upload_file(local_path: str | Path, bucket: str, key: str) -> str:
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {local_path}")

    content_type, _ = mimetypes.guess_type(str(local_path))
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    s3 = get_s3_client()
    if extra_args:
        s3.upload_file(str(local_path), bucket, key, ExtraArgs=extra_args)
    else:
        s3.upload_file(str(local_path), bucket, key)

    return f"s3://{bucket}/{key}"


def upload_json(data: dict[str, Any], bucket: str, key: str) -> str:
    s3 = get_s3_client()
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    return f"s3://{bucket}/{key}"