import pysftp
from pathlib import Path
from logger import log_result
from config import cargar_config

config = cargar_config()

def enviar_sftp(file_path: Path):
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(config["SFTP"]["HOST"], username=config["SFTP"]["USER"], password=config["SFTP"]["PASSWORD"], cnopts=cnopts) as sftp:
            sftp.put(file_path)
        log_result(f"Archivo {file_path.name} enviado por SFTP.")
    except Exception as e:
        log_result(f"Error al enviar archivo por SFTP: {e}")
        raise e
