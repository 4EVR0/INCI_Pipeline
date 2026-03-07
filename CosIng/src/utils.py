import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Optional


def setup_logger(log_dir: Path) -> logging.Logger:
    logger = logging.getLogger("cosing_collector")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    log_file = log_dir / f"cosing_collect_{datetime.now():%Y%m%d_%H%M%S}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


def first_or_none(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def join_list(value, sep=" | "):
    if isinstance(value, list):
        return sep.join(str(v).strip() for v in value if str(v).strip())
    if value is None:
        return None
    return str(value).strip()


def dump_json(data, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_json(path: Path, default=None):
    if not path.exists():
        return {} if default is None else default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_name(value: str):
    if value is None:
        return None
    text = str(value).strip().upper()
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def normalize_item_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    while "  " in text:
        text = text.replace("  ", " ")
    return text