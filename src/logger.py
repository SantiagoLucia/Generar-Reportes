import logging
from pathlib import Path

def log_result(result: str) -> None:
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename=Path("../logs/app.log"), level=logging.INFO, format='%(asctime)s - %(message)s')
    logger.info(result)

def format_time(seconds: float) -> tuple:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60
    return hours, minutes, seconds
