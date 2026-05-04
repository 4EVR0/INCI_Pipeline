from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline.silver_mapping.kcia_cosing.s3_io import upload_file, upload_json

from .config import GoldSettings, get_gold_settings
from .transform import load_matched_final_csv, transform_matched_final_to_gold
from .write_iceberg import write_gold_to_iceberg


GOLD_CSV_NAME = "kcia_cosing_gold_ingredients.csv"


def _gold_s3_prefix(settings: GoldSettings) -> str:
    return f"{settings.s3_gold_prefix}/kcia_cosing/batch_job={settings.batch_job}"


def _write_csv(df, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def run_gold_pipeline(settings: GoldSettings, upload_s3: bool = True) -> dict[str, object]:
    if not settings.silver_matched_path.exists():
        raise FileNotFoundError(f"Silver matched_final 없음: {settings.silver_matched_path}")

    raw = load_matched_final_csv(settings.silver_matched_path)
    gold_df = transform_matched_final_to_gold(raw)
    gold_df["batch_job"] = settings.batch_job
    gold_df["batch_date"] = settings.batch_date.isoformat()

    batch_dir = settings.gold_output_dir / f"batch_job={settings.batch_job}"
    local_csv = batch_dir / GOLD_CSV_NAME
    _write_csv(gold_df, local_csv)

    result: dict[str, object] = {
        "local_csv": local_csv,
        "row_count": len(gold_df),
        "s3_uri": None,
        "manifest_uri": None,
        "success_uri": None,
    }

    write_gold_to_iceberg(gold_df, settings)

    if not upload_s3:
        return result

    if not settings.s3_bucket:
        raise ValueError("S3 업로드에는 S3_BUCKET 이 필요합니다.")

    prefix = _gold_s3_prefix(settings)
    s3_key_csv = f"{prefix}/{GOLD_CSV_NAME}"
    s3_uri = upload_file(local_csv, settings.s3_bucket, s3_key_csv)
    result["s3_uri"] = s3_uri

    now_utc = datetime.now(timezone.utc).isoformat()
    manifest = {
        "pipeline": "kcia_cosing_gold_ingredients",
        "batch_month": settings.batch_month,
        "batch_job": settings.batch_job,
        "source_silver_csv": str(settings.silver_matched_path),
        "generated_at_utc": now_utc,
        "row_count": len(gold_df),
        "outputs": {"gold_ingredients": s3_uri},
    }
    manifest_uri = upload_json(manifest, settings.s3_bucket, f"{prefix}/manifest.json")
    success_payload = {
        "status": "SUCCESS",
        "pipeline": "kcia_cosing_gold_ingredients",
        "batch_month": settings.batch_month,
        "batch_job": settings.batch_job,
        "generated_at_utc": now_utc,
    }
    success_uri = upload_json(success_payload, settings.s3_bucket, f"{prefix}/_SUCCESS.json")
    result["manifest_uri"] = manifest_uri
    result["success_uri"] = success_uri

    meta_path = batch_dir / "manifest.json"
    meta_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return result


def main():
    import argparse

    p = argparse.ArgumentParser(
        description="최신 실버 배치 matched_final → Gold ingredients CSV (+ optional S3)"
    )
    p.add_argument("--no-upload", action="store_true", help="로컬 CSV만 생성하고 S3 업로드 생략")
    args = p.parse_args()

    settings = get_gold_settings()
    out = run_gold_pipeline(settings, upload_s3=not args.no_upload)

    print("=== KCIA ↔ CosIng Gold (ingredients) ===")
    print(f"batch (latest silver): {settings.batch_month}")
    print(f"source: {settings.silver_matched_path}")
    print(f"local: {out['local_csv']}")
    print(f"rows: {out['row_count']}")
    if out.get("s3_uri"):
        print(f"s3: {out['s3_uri']}")
        print(f"manifest: {out['manifest_uri']}")
        print(f"_SUCCESS: {out['success_uri']}")


if __name__ == "__main__":
    main()
