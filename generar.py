import sqlalchemy
import multiprocessing as mp
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED
from config import CON_URL

PATH = Path.cwd()
engine = sqlalchemy.create_engine(CON_URL)


def obtener_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--consulta", default="all", help="Archivo SQL")
    parser.add_argument("-f", "--formato", default="xlsx", help="Fomato (csv/xlsx)")
    parser.add_argument("-z", "--zip", action="store_true", help="Comprimir en zip")
    return parser.parse_args()


def generar_reporte(sql_path: Path, format_: str) -> None:
    with engine.connect() as conn:
        file = open(sql_path, "r").read()
        print(f"realizando consulta {sql_path.name}")
        query = sqlalchemy.text(file)
        data = pd.read_sql(query, con=conn)
        print(f"consulta realizada {sql_path.name}")

    if format_ == "xlsx":
        data.to_excel(sql_path.name.replace("sql", format_), index=False)
    if format_ == "csv":
        data.to_csv(sql_path.name.replace("sql", format_), index=False, sep=";")


def comprimir() -> None:
    with ZipFile(
        file="reportes.zip", mode="w", compression=ZIP_DEFLATED, compresslevel=9
    ) as zip_:
        for pattern in ("*.xlsx", "*.csv"):
            for file_path in PATH.glob(pattern):
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
                    args=(
                        path,
                        args.formato,
                    ),
                )
                for path in PATH.glob("*.sql")
            ]
            for process in processes:
                process.wait()
        else:
            path = PATH / args.consulta
            pool.apply_async(
                generar_reporte,
                args=(
                    path,
                    args.formato,
                ),
            )

    if args.zip:
        comprimir()

    tiempo_fin = datetime.now()
    tiempo_total = tiempo_fin - tiempo_inicio
    print(f"hora fin: {tiempo_fin.strftime('%H:%M')}")
    print(f"Finalizado en {str(tiempo_total).split('.')[0]}")


if __name__ == "__main__":
    main()
