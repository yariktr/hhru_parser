import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(level_console: int = logging.INFO, level_file: int = logging.DEBUG) -> None:
    logger = logging.getLogger()
    if logger.handlers:
        return 

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(level_console)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    fh = RotatingFileHandler(logs_dir / "run.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(level_file)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
