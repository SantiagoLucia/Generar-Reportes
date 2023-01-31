import sqlalchemy
import multiprocessing as mp
from argparse import ArgumentParser
import pandas as pd
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED
from config import CON_URL
from dataclasses import dataclass, field
from enum import Enum, auto

engine = sqlalchemy.create_engine(CON_URL)

PATH = Path.cwd()


class Formato(Enum):
    XLSX = auto()
    CSV = auto()


@dataclass
class Reporte:
    nombre: str
    formato: Formato
    sql_path: Path
    generado: bool = False
    
    def generar(self) -> None:
        with engine.connect() as con:
            query = open(self.sql_path, "r").read()
            data = pd.read_sql(sql=query, con=con)
            if self.formato is Formato.XLSX:
                data.to_excel(f"{self.nombre}.xlsx", index=False)
            if self.formato is Formato.CSV:
                data.to_csv(f"{self.nombre}.csv", index=False, sep=';')
            self.generado = True
        return self


@dataclass
class ListaReportes:
    reportes: list[Reporte] = field(default_factory=list)
        
    def agregar_reporte(self, reporte: Reporte) -> None:
        self.reportes.append(reporte)
                
    def filtrar_reportes(self, formato: Formato) -> list[Reporte]:
        return [reporte for reporte in self.reportes if reporte.formato is formato]
        
    def comprimir_reportes(self) -> None:
        with ZipFile(file="reportes.zip", 
                mode="w", compression=ZIP_DEFLATED, 
                compresslevel=9) as zip_:        
            for reporte in self.reportes:
                zip_.write(reporte.nombre)
                    
    def all(self):
        return self.reportes
    
    def generar(self) -> None:
        with mp.Pool() as pool:
            return pool.map(Reporte.generar, self.reportes) 


def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--consulta", default="all", help="Archivo SQL")
    parser.add_argument("-f", "--formato", default="xlsx", help="Fomato (csv/xlsx)")
    parser.add_argument("-z", "--zip", action='store_true', help="Comprimir en zip")   
    args = parser.parse_args()

    lista_reportes = ListaReportes()
    formato = Formato.CSV if args.formato == "csv" else Formato.XLSX 
    
    if args.consulta == "all":
        for path in PATH.glob("*.sql"):
            reporte = Reporte(
                nombre=path.name.split(".")[0],
                formato=formato,
                sql_path=path
            )
            lista_reportes.agregar_reporte(reporte)
    else:
        reporte = Reporte(
            nombre=args.consulta.split(".")[0],
            formato=formato,
            sql_path=PATH / args.consulta
        )
        lista_reportes.agregar_reporte(reporte)
    
    lista_reportes.generar()
    
    if args.zip: lista_reportes.comprimir_reportes()
        
if __name__ == "__main__":
    main()