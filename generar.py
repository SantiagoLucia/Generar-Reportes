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
    parser.add_argument("-z", "--zip", action='store_true', help="Comprimir en zip")   
    return parser.parse_args()

def generar_reporte(sql_path, format_):
    with engine.connect() as conn:
        query = open(sql_path, 'r').read()
        print(f"realizando consulta {sql_path.name}")
        data = pd.read_sql(query, con=conn)
        print(f"consulta realizada {sql_path.name}")

    if format_ == "xlsx":
        data.to_excel(sql_path.name.replace("sql", format_), index=False)
    if format_ == "csv":
        data.to_csv(sql_path.name.replace("sql", format_), index=False, sep=';')
    
def comprimir():
    with ZipFile(
            file="reportes.zip", 
            mode="w", 
            compression=ZIP_DEFLATED, 
            compresslevel=9) as zip_:
        
        for pattern in ("*.xlsx", "*.csv"):
            for file_path in PATH.glob(pattern):
                zip_.write(file_path.name)

def create_process(sql_path, format_, process_list):
        process = mp.Process(target=generar_reporte, args=(sql_path, format_,))
        process_list.append(process)
        process.start()

if __name__ == "__main__":
    
    args = obtener_args()
    processes = []

    tiempo_inicio = datetime.now()
    print(f"hora inicio: {tiempo_inicio.strftime('%H:%M')}")
  
    if args.consulta == "all":
        for path in PATH.glob("*.sql"):
            create_process(path, args.formato, processes)
    else:
        path = PATH / args.consulta       
        create_process(path, args.formato, processes)

    for _ in processes:
        _.join()

    tiempo_fin = datetime.now()
    tiempo_total = tiempo_fin - tiempo_inicio
    print(f"hora fin: {tiempo_fin.strftime('%H:%M')}")
    print(f"Finalizado en {str(tiempo_total).split('.')[0]}")
    
    if not args.zip:
        comprimir()