import logging
from pathlib import Path

LOG_FILE = Path("src/nbu_etl.log")


def setup_logger(name: str) -> logging.Logger:
    """Configure and return a logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
        ],
    )
    return logging.getLogger(name)
