import sqlalchemy
import multiprocessing as mp
import argparse
import pandas as pd
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import configparser
import logging
import time
import pysftp
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


config = configparser.ConfigParser()
config.read("config.ini")
try:
    DB_USER = config["ORACLE"]["USER"]
    DB_PASSWORD = config["ORACLE"]["PASSWORD"]
    DB_HOST = config["ORACLE"]["HOST"]
    DB_PORT = config["ORACLE"]["PORT"]
    DB_SERVICE_NAME = config["ORACLE"]["SERVICE_NAME"]
    LOGFILE = config["LOGGING"]["LOGFILE"]
    CHUNK_SIZE = int(config["GENERAL"]["CHUNK_SIZE"])
    PROCESSES = int(config["GENERAL"]["PROCESSES"])
except KeyError as e:
    raise KeyError(f"Falta la clave {e} en el archivo de configuración.")

CON_URL = f"oracle+oracledb://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/?service_name={DB_SERVICE_NAME}"
PATH = Path.cwd()

logging.basicConfig(filename=LOGFILE, level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(name=LOGFILE)


def log_result(result: str) -> None:
    """Loggear el resultado de la generación de un reporte."""
    logger.info(result)

def format_time(seconds: float) -> tuple:
    """Formatear los segundos en horas, minutos y segundos."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60
    return hours, minutes, seconds

def obtener_args():
    """
    Parsear los argumentos de línea de comandos.

    Retorna:
        argparse.Namespace: Un objeto con los argumentos parseados.
    """
    parser = argparse.ArgumentParser(description="Procesar argumentos para la generación de reportes.")
    parser.add_argument("--consulta", default="all", help="Archivo SQL a utilizar para la consulta.")
    parser.add_argument("--zip", action="store_true", help="Comprimir el resultado en un archivo zip.")
    parser.add_argument("--sftp", help="Enviar el archivo por SFTP si fue previamente comprimido.")
    parser.add_argument("--smtp", help="Enviar el archivo por correo electrónico si fue previamente comprimido.")
    return parser.parse_args()

def generar_reporte(sql_path: Path) -> str:
    """
    Generar un reporte a partir de una consulta SQL y exportarlo a un archivo CSV.

    Args:
        sql_path (Path): Ruta del archivo SQL.
    """
    cantidad_registros = 0
    try:
        # Crear el motor de SQLAlchemy con opciones de optimización de memoria
        engine = sqlalchemy.create_engine(CON_URL).execution_options(stream_results=True)
        
        with engine.connect() as conn:
            with open(sql_path, "r") as file:
                query = sqlalchemy.text(file.read().rstrip(";"))
            
            export_name = sql_path.name.replace("sql", "csv")
            output_path = PATH / "salida" / export_name
            output_path.parent.mkdir(parents=True, exist_ok=True)

            first_chunk = True
            start_time = time.perf_counter()
            for chunk_data in pd.read_sql(query, conn, chunksize=CHUNK_SIZE):
                chunk_data.to_csv(
                    output_path,
                    index=False,
                    sep=";",
                    mode="a",
                    header=first_chunk,
                )
                first_chunk = False
                cantidad_registros += len(chunk_data)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            hours, minutes, seconds = format_time(elapsed_time)
        return f"Reporte {export_name} generado con {cantidad_registros} registros en {hours} horas, {minutes} minutos y {seconds:.2f} segundos."        
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
            for file in (PATH / "salida").glob("*.csv"):
                zip_.write(file, arcname=file.name)
        zip_size = Path("reportes.zip").stat().st_size
        zip_size = zip_size / (1024 * 1024)
        log_result(f"Archivo comprimido reportes.zip de {zip_size:.2f} MB generado.")    
    except Exception as e:
        print(f"Error al comprimir archivos: {e}")

def enviar_sftp(file_path: Path) -> None:
    """
    Enviar un archivo por SFTP a un servidor remoto.

    Args:
        file_path (Path): Ruta del archivo a enviar.
    """
    try:
        SFTP_HOST = config["SFTP"]["HOST"]
        SFTP_USER = config["SFTP"]["USER"]
        SFTP_PASSWORD = config["SFTP"]["PASSWORD"]
        SFTP_DIR = config["SFTP"]["DIR"]
    except KeyError as e:
        raise KeyError(f"Falta la clave {e} en el archivo de configuración.")
        
    try:
        with pysftp.Connection(SFTP_HOST, username=SFTP_USER, password=SFTP_PASSWORD) as sftp:
            with sftp.cd(SFTP_DIR):
                sftp.put(file_path)
        log_result(f"Archivo {file_path.name} enviado por SFTP.")        
    except Exception as e:
        log_result(f"Error al enviar archivo por SFTP: {e}")

def enviar_email_con_adjunto(file_path: Path) -> None:
    """
    Enviar un correo electrónico con un archivo adjunto.
    """
    try:
        SMTP_HOST = config["SMTP"]["HOST"]
        SMTP_PORT = config["SMTP"]["PORT"]
        SMTP_USER = config["SMTP"]["USER"]
        SMTP_PASSWORD = config["SMTP"]["PASSWORD"]
        SMTP_TO = config["SMTP"]["TO"]
        SMTP_SUBJECT = config["SMTP"]["SUBJECT"]
        SMTP_BODY = config["SMTP"]["BODY"]
    except KeyError as e:
        raise KeyError(f"Falta la clave {e} en el archivo de configuración.")
        
    try:
        # Crear el objeto del mensaje
        msg = MIMEMultipart()
        msg['From'] = SMTP_USER
        msg['To'] = SMTP_TO
        msg['Subject'] = SMTP_SUBJECT

        # Agregar el cuerpo del correo
        msg.attach(MIMEText(SMTP_BODY, 'plain'))

        with open(file_path, 'rb') as attachment:
            # Crear el objeto MIMEBase
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={file_path.name}')
            # Adjuntar el archivo al mensaje
            msg.attach(part)

        # Conectar al servidor SMTP
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()  # Usar TLS (Transport Layer Security)
            # Autenticarse
            server.login(SMTP_USER, SMTP_PASSWORD)
            # Enviar el correo
            text = msg.as_string()
            server.sendmail(SMTP_USER, SMTP_TO, text)    
    except Exception as e:
        log_result(f"Error al enviar correo electrónico: {e}")

def main() -> None:
    """
    Función principal para ejecutar la generación de reportes y la compresión de archivos.
    """
    args = obtener_args()

    try:
        with mp.Pool(processes=PROCESSES) as pool:  # Aumentar el número de procesos a 4
            if args.consulta == "all":
                processes = [
                    pool.apply_async(generar_reporte, args=(path,), callback=log_result)
                    for path in (PATH / "consultas").glob("*.sql")
                ]
                for process in processes:
                    process.get()  # Manejar adecuadamente el resultado de apply_async
            else:
                path = PATH / "consultas" / args.consulta
                pool.apply_async(generar_reporte, args=(path,)).get()  # Manejar adecuadamente el resultado de apply_async

        if args.zip:
            comprimir()
            file_path = PATH / "reportes.zip"
            if args.sftp:
                enviar_sftp(file_path)
            if args.smtp:
                enviar_email_con_adjunto(file_path)    

    except Exception as e:
        log_result(f"Error durante la ejecución: {e}")

if __name__ == "__main__":
    main()