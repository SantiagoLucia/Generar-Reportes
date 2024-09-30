from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from logger import log_result

def comprimir(directorio_entrada: Path, archivo_salida: Path):
    try:
        archivos_csv = list(directorio_entrada.glob("*.csv"))
        if not archivos_csv:
            log_result("No se encontraron archivos CSV para comprimir.")
            return

        with ZipFile(archivo_salida, mode="w", compression=ZIP_DEFLATED) as zipf:
            for file in archivos_csv:
                zipf.write(file, arcname=file.name)
        
        zip_size = archivo_salida.stat().st_size / (1024 * 1024)
        log_result(f"Archivo comprimido {archivo_salida.name} de {zip_size:.2f} MB generado.")
    except FileNotFoundError:
        log_result(f"El directorio {directorio_entrada} no existe.")
    except Exception as e:
        log_result(f"Error al comprimir archivos: {e}")
