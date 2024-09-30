import logging
from pathlib import Path
from config import cargar_config

config = cargar_config()

def log_result(result: str) -> None:
    """
    Loguea el resultado de una operación en el archivo de log.
    
    Args:
        result (str): Mensaje a log
    """
    logger = logging.getLogger()
    logging.basicConfig(filename=Path(config["RUTAS"]["LOGFILE"]), level=logging.INFO, format='%(asctime)s - %(message)s')
    logger.info(result)

def format_time(seconds: float) -> tuple:
    """
    Convierte los segundos en horas, minutos y segundos.
    
    Args:
        seconds (float): Segundos a convertir.
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60
    return hours, minutes, seconds
