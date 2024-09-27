from zipfile import ZipFile, ZIP_DEFLATED
from pathlib import Path
from logger import log_result

def comprimir():
    try:
        with ZipFile(Path("../salida/reportes.zip"), mode="w", compression=ZIP_DEFLATED) as zipf:
            for file in (Path("../salida")).glob("*.csv"):
                zipf.write(file, arcname=file.name)
        zip_size = Path("../salida/reportes.zip").stat().st_size / (1024 * 1024)
        log_result(f"Archivo comprimido reportes.zip de {zip_size:.2f} MB generado.")
    except Exception as e:
        log_result(f"Error al comprimir archivos: {e}")
        raise e
