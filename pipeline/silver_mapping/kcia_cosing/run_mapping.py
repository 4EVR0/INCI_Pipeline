from __future__ import annotations

import logging

from oliveyoung_common.logging import job_unit, setup_logging

from .config import get_settings
from .pipeline import run_and_save

setup_logging("inci-silver-mapping")

logger = logging.getLogger(__name__)


def main():
    settings = get_settings()
    with job_unit(
        logger,
        job="kcia_cosing_silver_mapping",
        run_id=settings.run_id,
        batch_job=settings.batch_job,
        batch_month=settings.batch_month,
        input_mode=settings.input_mode,
    ):
        result = run_and_save(settings)

    print("=== KCIA ↔ CosIng Silver Mapping Complete ===")

    print("\n[LOCAL OUTPUTS]")
    for name, path in result["local_paths"].items():
        print(f"{name}: {path}")

    print("\n[S3 OUTPUTS]")
    for name, uri in result["s3_paths"].items():
        print(f"{name}: {uri}")

    print("\n[META]")
    for name, uri in result["meta_paths"].items():
        print(f"{name}: {uri}")


if __name__ == "__main__":
    main()
