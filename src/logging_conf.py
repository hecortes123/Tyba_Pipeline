"""Logging estructurado y consistente para todo el pipeline."""
from __future__ import annotations

import logging
import sys


def get_logger(name: str = "tyba") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:  # evita handlers duplicados en re-imports
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
