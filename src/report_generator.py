import sqlalchemy
import pandas as pd
from pathlib import Path
import time
from logger import log_result, format_time
from config import cargar_config

config = cargar_config()
CHUNK_SIZE = int(config["GENERAL"]["CHUNK_SIZE"])
CON_URL = config["ORACLE"]["CON_URL"]

def generar_reporte(sql_path: Path) -> str:
    cantidad_registros = 0
    try:
        engine = sqlalchemy.create_engine(CON_URL).execution_options(stream_results=True)
        
        with engine.connect() as conn:
            with open(sql_path, "r") as file:
                query = sqlalchemy.text(file.read().rstrip(";"))

            export_name = sql_path.name.replace("sql", "csv")
            output_path = Path("../salida") / export_name
            output_path.parent.mkdir(parents=True, exist_ok=True)

            first_chunk = True
            start_time = time.perf_counter()
            for chunk_data in pd.read_sql(query, conn, chunksize=CHUNK_SIZE):
                chunk_data.to_csv(output_path, index=False, sep=";", mode="a", header=first_chunk, encoding="windows-1252")
                first_chunk = False
                cantidad_registros += len(chunk_data)
            end_time = time.perf_counter()
            hours, minutes, seconds = format_time(end_time - start_time)
            
        return f"Reporte {export_name} generado con {cantidad_registros} registros en {hours}h {minutes}m {seconds:.2f}s."
    except Exception as e:
        log_result(f"Error al generar el reporte: {e}")
