"""
KCIA Bronze Iceberg write 모듈

테이블:
    inci_db.kcia_bronze_current  — overwrite (최신 배치만 유지)
    inci_db.kcia_bronze_history  — append    (누적 이력 보관)

S3 위치:
    s3://{S3_BUCKET}/inci_iceberg/kcia_bronze/current/
    s3://{S3_BUCKET}/inci_iceberg/kcia_bronze/history/
"""

from __future__ import annotations

from dataclasses import asdict

import pandas as pd
import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType

from common.iceberg_config import INCIIceberg

# ==========================================
# KCIA Bronze 스키마
# ==========================================

KCIA_BRONZE_SCHEMA = Schema(
    NestedField(1,  "ingredient_code", StringType(),      required=True),
    NestedField(2,  "std_name_ko",     StringType(),      required=False),
    NestedField(3,  "std_name_en",     StringType(),      required=False),
    NestedField(4,  "cas_no",          StringType(),      required=False),
    NestedField(5,  "old_name_ko",     StringType(),      required=False),
    NestedField(6,  "as_of_date",      StringType(),      required=False),
    NestedField(7,  "source",          StringType(),      required=False),
    NestedField(8,  "ingest_date",     StringType(),      required=False),
    NestedField(9,  "batch_month",     StringType(),      required=False),
    NestedField(10, "batch_id",        StringType(),      required=False),
    NestedField(11, "batch_job",       StringType(),      required=False),
    NestedField(12, "batch_date",      TimestamptzType(), required=False),
)


# ==========================================
# 내부 유틸
# ==========================================

def _load_or_create_table(catalog, identifier: str, location: str):
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        return catalog.create_table(
            identifier=identifier,
            schema=KCIA_BRONZE_SCHEMA,
            location=location,
            partition_spec=PartitionSpec(),
        )


def _build_arrow_table(df: pd.DataFrame, table) -> pa.Table:
    iceberg_arrow_schema = table.schema().as_arrow()

    work_df = df.copy()
    for col in iceberg_arrow_schema.names:
        if col not in work_df.columns:
            work_df[col] = None

    if "batch_date" in work_df.columns:
        work_df["batch_date"] = pd.to_datetime(work_df["batch_date"], utc=True, errors="coerce")

    arrow_dict: dict[str, pa.Array] = {}
    for field in iceberg_arrow_schema:
        values = work_df[field.name].tolist() if field.name in work_df.columns else [None] * len(work_df)
        arrow_dict[field.name] = pa.array(values, type=field.type)

    return pa.table(arrow_dict, schema=iceberg_arrow_schema)


# ==========================================
# Public API
# ==========================================

def write_kcia_bronze_to_iceberg(bronze_rows: list, settings) -> None:
    """
    KCIA Bronze 데이터를 Iceberg 테이블에 기록합니다.

    Args:
        bronze_rows : transform_to_bronze() 반환값 (KciaBronzeRow 리스트)
        settings    : kcia_pipeline.config.Settings 인스턴스
    """
    if not bronze_rows:
        print("   KCIA Bronze: 데이터 없음 — Iceberg write 건너뜀")
        return

    df = pd.DataFrame([asdict(row) for row in bronze_rows])
    base = f"s3://{settings.s3_bucket}/inci_iceberg"
    catalog = INCIIceberg.get_catalog()

    # current (overwrite)
    current_table = _load_or_create_table(
        catalog,
        INCIIceberg.KCIA_BRONZE_CURRENT_TABLE,
        f"{base}/kcia_bronze/current",
    )
    current_table.overwrite(_build_arrow_table(df, current_table))
    print(f"   Iceberg overwrite: {INCIIceberg.KCIA_BRONZE_CURRENT_TABLE} ({len(df)}건)")

    # history (append)
    history_table = _load_or_create_table(
        catalog,
        INCIIceberg.KCIA_BRONZE_HISTORY_TABLE,
        f"{base}/kcia_bronze/history",
    )
    history_table.append(_build_arrow_table(df, history_table))
    print(f"   Iceberg append:    {INCIIceberg.KCIA_BRONZE_HISTORY_TABLE} ({len(df)}건)")
