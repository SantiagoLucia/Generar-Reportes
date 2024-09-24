import sqlalchemy
import multiprocessing as mp
import argparse
import pandas as pd
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import configparser

config = configparser.ConfigParser()
try:
    config.read("config.ini")
    USER = config["ORACLE"]["USER"]
    PASSWORD = config["ORACLE"]["PASSWORD"]
    HOST = config["ORACLE"]["HOST"]
    PORT = config["ORACLE"]["PORT"]
    SERVICE_NAME = config["ORACLE"]["SERVICE_NAME"]
except KeyError as e:
    raise KeyError(f"Falta la clave {e} en el archivo de configuración.")

CON_URL = f"oracle+oracledb://{USER}:{PASSWORD}@{HOST}:{PORT}/?service_name={SERVICE_NAME}"

PATH = Path.cwd()
CHUNK_SIZE = 10000

def obtener_args():
    """
    Parsear los argumentos de línea de comandos.

    Retorna:
        argparse.Namespace: Un objeto con los argumentos parseados.
    """
    parser = argparse.ArgumentParser(description="Procesar argumentos para la generación de reportes.")
    parser.add_argument("--consulta", default="all", help="Archivo SQL a utilizar para la consulta.")
    parser.add_argument("--zip", action="store_true", help="Comprimir el resultado en un archivo zip.")
    return parser.parse_args()

def generar_reporte(sql_path: Path) -> None:
    """
    Generar un reporte a partir de una consulta SQL y exportarlo a un archivo CSV.

    Args:
        sql_path (Path): Ruta del archivo SQL.
    """
    try:
        # Crear el motor de SQLAlchemy con opciones de optimización de memoria
        engine = sqlalchemy.create_engine(CON_URL).execution_options(stream_results=True)
        
        with engine.connect() as conn:
            with open(sql_path, "r") as file:
                query = sqlalchemy.text(file.read())
            
            export_name = sql_path.name.replace("sql", "csv")
            output_path = Path(f"./salida/{export_name}")
            output_path.parent.mkdir(parents=True, exist_ok=True)

            first_chunk = True
            for chunk_data in pd.read_sql(query, conn, chunksize=CHUNK_SIZE):
                chunk_data.to_csv(
                    output_path,
                    index=False,
                    sep=";",
                    mode="a",
                    header=first_chunk,
                )
                first_chunk = False
    except Exception as e:
        print(f"Error al generar el reporte: {e}")

def comprimir() -> None:
    """
    Comprimir todos los archivos CSV en el directorio actual en un archivo ZIP.
    """
    try:
        with ZipFile(
            file="reportes.zip", mode="w", compression=ZIP_DEFLATED, compresslevel=9
        ) as zip_:
            for file_path in PATH.glob("*.csv"):
                zip_.write(file_path, arcname=file_path.name)
    except Exception as e:
        print(f"Error al comprimir archivos: {e}")

def main() -> None:
    """
    Función principal para ejecutar la generación de reportes y la compresión de archivos.
    """
    args = obtener_args()

    try:
        with mp.Pool(processes=4) as pool:  # Aumentar el número de procesos a 4
            if args.consulta == "all":
                processes = [
                    pool.apply_async(generar_reporte, args=(path,))
                    for path in Path("./consultas").glob("*.sql")
                ]
                for process in processes:
                    process.get()  # Manejar adecuadamente el resultado de apply_async
            else:
                path = Path("./consultas") / args.consulta
                pool.apply_async(generar_reporte, args=(path,)).get()  # Manejar adecuadamente el resultado de apply_async

        if args.zip:
            comprimir()
    except Exception as e:
        print(f"Error durante la ejecución: {e}")

if __name__ == "__main__":
    main()