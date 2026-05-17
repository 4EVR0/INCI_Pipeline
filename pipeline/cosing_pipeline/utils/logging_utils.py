import logging


def setup_logger(name: str = "cosing_pipeline") -> logging.Logger:
    return logging.getLogger(name)
