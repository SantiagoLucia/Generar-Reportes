from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from logger import log_result
from config import cargar_config
from datetime import datetime

config = cargar_config()
path_salida = Path(config["RUTAS"]["SALIDA"])

def eliminar_archivos_csv():
    """
    Elimina los archivos CSV generados en la carpeta de salida.
    """
    try:
        archivos_csv = list(path_salida.glob("*.csv"))
        for file in archivos_csv:
            file.unlink()
        log_result("Archivos CSV eliminados.")
    except FileNotFoundError:
        log_result(f"El directorio {path_salida} no existe.")
    except Exception as e:
        log_result(f"Error al eliminar archivos: {e}")

def comprimir():
    """
    Comprime los archivos CSV generados en la carpeta de salida en un archivo ZIP.
    """
    archivo_salida = path_salida / f"reportes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    try:
        archivos_csv = list(path_salida.glob("*.csv"))
        if not archivos_csv:
            log_result("No se encontraron archivos CSV para comprimir.")
            return

        with ZipFile(archivo_salida, mode="w", compression=ZIP_DEFLATED) as zipf:
            for file in archivos_csv:
                zipf.write(file, arcname=file.name)
        
        zip_size = archivo_salida.stat().st_size / (1024 * 1024)
        log_result(f"Archivo comprimido {archivo_salida.name} de {zip_size:.2f} MB generado.")
    except FileNotFoundError:
        log_result(f"El directorio {path_salida} no existe.")
    except Exception as e:
        log_result(f"Error al comprimir archivos: {e}")
