import logging


def setup_logger(name: str = "kcia_pipeline") -> logging.Logger:
    return logging.getLogger(name)
