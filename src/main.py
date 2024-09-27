import multiprocessing as mp
from pathlib import Path
from config import cargar_config
from report_generator import generar_reporte
from file_utils import comprimir
from sftp_utils import enviar_sftp
from email_utils import enviar_email_con_adjunto
from logger import log_result
import argparse

config = cargar_config()

def obtener_args():
    parser = argparse.ArgumentParser(description="Procesar argumentos para la generación de reportes.")
    parser.add_argument("--consulta", default="all", help="Archivo SQL a utilizar para la consulta.")
    parser.add_argument("--zip", action="store_true", help="Comprimir el resultado en un archivo zip.")
    parser.add_argument("--sftp", action="store_true", help="Enviar el archivo por SFTP si fue previamente comprimido.")
    parser.add_argument("--smtp", action="store_true", help="Enviar el archivo por correo electrónico si fue previamente comprimido.")
    return parser.parse_args()

def main():
    args = obtener_args()

    try:
        processes = int(config["GENERAL"]["PROCESSES"])
        with mp.Pool(processes=processes) as pool:
            if args.consulta == "all":
                processes = [
                    pool.apply_async(generar_reporte, args=(path,), callback=log_result)
                    for path in Path("../consultas").glob("*.sql")
                ]

                for process in processes:
                    process.get()
            else:
                pool.apply_async(generar_reporte, args=(Path("../consultas") / args.consulta,)).get()

        if args.zip:
            comprimir()
            if args.sftp:
                enviar_sftp(Path("../salida/reportes.zip"))
            if args.smtp:
                enviar_email_con_adjunto(Path("../salida/reportes.zip"))

    except Exception as e:
        log_result(f"Error durante la ejecución: {e}")

if __name__ == "__main__":
    main()
