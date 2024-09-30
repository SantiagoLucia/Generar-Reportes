import configparser
from pathlib import Path

def cargar_config(ruta_config: str = "../conf/config.ini"):
    """
    Carga la configuración desde un archivo INI.

    Args:
        ruta_config (str): Ruta al archivo de configuración.

    Returns:
        configparser.ConfigParser: Objeto ConfigParser con la configuración cargada.
    """
    config = configparser.ConfigParser()
    ruta = Path(ruta_config)
    
    if not ruta.exists():
        raise FileNotFoundError(f"El archivo de configuración {ruta_config} no existe.")
    
    config.read(ruta)
    return config