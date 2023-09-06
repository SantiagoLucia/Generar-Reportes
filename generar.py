import sqlalchemy
import multiprocessing as mp
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED
from config import CON_URL

PATH = Path.cwd()
engine = sqlalchemy.create_engine(CON_URL).execution_options(stream_results=True)


def obtener_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--consulta", default="all", help="Archivo SQL")
    parser.add_argument("-z", "--zip", action="store_true", help="Comprimir en zip")
    return parser.parse_args()


def generar_reporte(sql_path: Path) -> None:
    with engine.connect() as conn:
        file = open(sql_path, "r").read()
        print(f"realizando consulta {sql_path.name}")
        query = sqlalchemy.text(file)
        export_name = sql_path.name.replace("sql", "csv")
        first_chunk = True
        for chunk_data in pd.read_sql(query, conn, chunksize=50000):
            if first_chunk:
                chunk_data.to_csv(
                    export_name, index=False, sep=";", mode="a", header=True
                )
                first_chunk = False
            chunk_data.to_csv(export_name, index=False, sep=";", mode="a", header=False)


def comprimir() -> None:
    with ZipFile(
        file="reportes.zip", mode="w", compression=ZIP_DEFLATED, compresslevel=9
    ) as zip_:
        for file_path in PATH.glob("*.csv"):
            zip_.write(file_path.name)


def main() -> None:
    args = obtener_args()

    tiempo_inicio = datetime.now()
    print(f"hora inicio: {tiempo_inicio.strftime('%H:%M')}")

    with mp.Pool() as pool:
        if args.consulta == "all":
            processes = [
                pool.apply_async(
                    generar_reporte,
                    args=(path,),
                )
                for path in PATH.glob("*.sql")
            ]
            for process in processes:
                process.wait()
        else:
            path = PATH / args.consulta
            pool.apply_async(
                generar_reporte,
                args=(path,),
            )

    if args.zip:
        comprimir()

    tiempo_fin = datetime.now()
    tiempo_total = tiempo_fin - tiempo_inicio
    print(f"hora fin: {tiempo_fin.strftime('%H:%M')}")
    print(f"Finalizado en {str(tiempo_total).split('.')[0]}")


if __name__ == "__main__":
    main()
