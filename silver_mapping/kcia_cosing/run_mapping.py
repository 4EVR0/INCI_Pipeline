from __future__ import annotations

from .config import get_settings
from .pipeline import run_and_save


def main():
    settings = get_settings()
    paths = run_and_save(settings)

    print("=== KCIA ↔ CosIng Silver Mapping Complete ===")
    for name, path in paths.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
