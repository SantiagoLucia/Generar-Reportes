import logging
from pathlib import Path
from config import cargar_config

config = cargar_config()

def log_result(result: str) -> None:
    logger = logging.getLogger()
    logging.basicConfig(filename=Path(config["RUTAS"]["LOGFILE"]), level=logging.INFO, format='%(asctime)s - %(message)s')
    logger.info(result)

def format_time(seconds: float) -> tuple:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60
    return hours, minutes, seconds
