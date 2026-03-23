import logging
import sys


def setup_logger(name: str = "kcia_pipeline") -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # 중복 방지

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger