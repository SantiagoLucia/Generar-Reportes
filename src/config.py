import configparser
from pathlib import Path

def cargar_config():
    config = configparser.ConfigParser()
    config.read(Path("../conf/config.ini"))
    return config
