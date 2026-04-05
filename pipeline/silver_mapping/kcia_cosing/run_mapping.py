from __future__ import annotations

from .config import get_settings
from .pipeline import run_and_save


def main():
    settings = get_settings()
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