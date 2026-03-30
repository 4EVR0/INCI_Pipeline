from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class BronzePaths:
    source: str
    batch_month: str
    local_dir: Path
    local_csv_path: Path
    local_metadata_path: Path
    s3_dir: str
    s3_csv_key: str
    s3_metadata_key: str


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_bronze_paths(
    source: str,
    batch_month: str,
    s3_prefix: str,
    file_name: str,
    local_root: Path | None = None,
) -> BronzePaths:
    if local_root is None:
        local_root = PROJECT_ROOT / "bronze" / source

    batch_dir_name = f"batch={batch_month}"

    local_dir = local_root / batch_dir_name
    local_csv_path = local_dir / file_name
    local_metadata_path = local_dir / "metadata.json"

    clean_prefix = s3_prefix.rstrip("/")
    s3_dir = f"{clean_prefix}/{batch_dir_name}"
    s3_csv_key = f"{s3_dir}/{file_name}"
    s3_metadata_key = f"{s3_dir}/metadata.json"

    return BronzePaths(
        source=source,
        batch_month=batch_month,
        local_dir=local_dir,
        local_csv_path=local_csv_path,
        local_metadata_path=local_metadata_path,
        s3_dir=s3_dir,
        s3_csv_key=s3_csv_key,
        s3_metadata_key=s3_metadata_key,
    )


def get_kcia_bronze_paths(batch_month: str, s3_prefix: str) -> BronzePaths:
    return build_bronze_paths(
        source="kcia",
        batch_month=batch_month,
        s3_prefix=s3_prefix,
        file_name="kcia_bronze.csv",
    )