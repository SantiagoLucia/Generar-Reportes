import pysftp
from pathlib import Path
from logger import log_result
from config import cargar_config

config = cargar_config()

def enviar_sftp():
    """
    Envía el archivo comprimido por SFTP.
    """
    path_salida = Path(config["RUTAS"]["SALIDA"])
    archivo_salida = path_salida / "reportes.zip"
    host = config["SFTP"]["HOST"]
    username = config["SFTP"]["USER"]
    password = config["SFTP"]["PASSWORD"]
    try:
        cnopts = pysftp.CnOpts()
        # Configurar las claves del host adecuadamente en lugar de desactivarlas
        # cnopts.hostkeys.load('path/to/known_hosts')
        
        with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as sftp:
            sftp.put(archivo_salida)
        log_result(f"Archivo {archivo_salida.name} enviado por SFTP.")
    except pysftp.ConnectionException as e:
        log_result(f"Error de conexión SFTP: {e}")
    except pysftp.CredentialException as e:
        log_result(f"Error de credenciales SFTP: {e}")
    except Exception as e:
        log_result(f"Error al enviar archivo por SFTP: {e}")
