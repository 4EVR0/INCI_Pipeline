from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd

from .config import Settings
from .normalizer import nullify


@dataclass
class LocatedInput:
    source: str
    path: Path
    s3_key: Optional[str] = None


def _read_csv(path: Path, usecols=None) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, usecols=usecols).fillna("")


def _latest_s3_key(bucket: str, prefix: str, client, ingest_date: Optional[str] = None) -> str:
    paginator = client.get_paginator("list_objects_v2")
    candidates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.lower().endswith(".csv"):
                continue
            if ingest_date and f"ingest_date={ingest_date}" not in key and ingest_date not in key:
                continue
            candidates.append((key, item["LastModified"]))
    if not candidates:
        raise FileNotFoundError(f"S3에서 CSV를 찾지 못했습니다. bucket={bucket}, prefix={prefix}, ingest_date={ingest_date}")
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


def _download_from_s3(bucket: str, key: str, local_path: Path, region_name: str) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    session = boto3.session.Session(region_name=region_name)
    s3 = session.client("s3")
    s3.download_file(bucket, key, str(local_path))
    return local_path


def locate_inputs(settings: Settings) -> tuple[LocatedInput, LocatedInput]:
    if settings.input_mode == "local":
        return (
            LocatedInput(source="local", path=settings.kcia_local_path),
            LocatedInput(source="local", path=settings.cosing_local_path),
        )

    if settings.input_mode != "s3":
        raise ValueError(f"지원하지 않는 MAPPING_INPUT_MODE 입니다: {settings.input_mode}")

    session = boto3.session.Session(region_name=settings.aws_region)
    s3 = session.client("s3")

    kcia_key = _latest_s3_key(
        bucket=settings.s3_bucket,
        prefix=settings.kcia_s3_prefix,
        client=s3,
        ingest_date=settings.kcia_ingest_date,
    )
    cosing_key = _latest_s3_key(
        bucket=settings.s3_bucket,
        prefix=settings.cosing_s3_prefix,
        client=s3,
        ingest_date=settings.cosing_ingest_date,
    )

    cache_dir = settings.data_dir / "cache"
    kcia_local = cache_dir / Path(kcia_key).name
    cosing_local = cache_dir / Path(cosing_key).name

    _download_from_s3(settings.s3_bucket, kcia_key, kcia_local, settings.aws_region)
    _download_from_s3(settings.s3_bucket, cosing_key, cosing_local, settings.aws_region)

    return (
        LocatedInput(source="s3", path=kcia_local, s3_key=kcia_key),
        LocatedInput(source="s3", path=cosing_local, s3_key=cosing_key),
    )


def load_kcia_csv(path: Path) -> pd.DataFrame:
    df = _read_csv(path)

    required_cols = ["ingredient_code", "std_name_ko", "std_name_en", "cas_no"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"KCIA CSV에 필요한 컬럼이 없습니다: {missing}")

    optional_cols = ["old_name_ko", "as_of_date", "source", "ingest_date", "batch_id"]
    for col in optional_cols:
        if col not in df.columns:
            df[col] = ""

    for col in [required_cols + optional_cols][0]:
        df[col] = df[col].astype(str)

    return df.copy()


def load_cosing_csv(path: Path) -> pd.DataFrame:
    candidate_cols = {
        "inci_name": ["inci_name", "std_name_en", "ingredient_name"],
        "substance_id": ["substance_id", "cosing_ing_id", "ingredient_id"],
        "cas_no": ["cas_no"],
        "function_names": ["function_names", "functions"],
        "cosmetic_restriction": ["cosmetic_restriction"],
        "other_restrictions": ["other_restrictions"],
        "identified_ingredient": ["identified_ingredient"],
        "status": ["status"],
        "source": ["source"],
        "ingest_date": ["ingest_date"],
        "batch_id": ["batch_id"],
    }

    df_head = pd.read_csv(path, nrows=3)
    rename_map = {}
    usecols = []
    for canonical, candidates in candidate_cols.items():
        found = next((c for c in candidates if c in df_head.columns), None)
        if found:
            rename_map[found] = canonical
            usecols.append(found)

    if "inci_name" not in rename_map.values():
        raise ValueError("CosIng CSV에 inci_name 계열 컬럼이 없습니다.")

    df = _read_csv(path, usecols=usecols).rename(columns=rename_map)

    for col in candidate_cols:
        if col not in df.columns:
            df[col] = ""

    return df.copy()


def write_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def normalize_output_nulls(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].apply(nullify)
    return out
