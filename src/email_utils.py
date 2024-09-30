import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from logger import log_result
from config import cargar_config

config = cargar_config()

def enviar_email_con_adjunto():
    path_salida = Path(config["RUTAS"]["SALIDA"])
    archivo_salida = path_salida / "reportes.zip"
    try:
        msg = MIMEMultipart()
        msg['From'] = config["SMTP"]["USER"]
        msg['To'] = config["SMTP"]["TO"]
        msg['Subject'] = config["SMTP"]["SUBJECT"]

        msg.attach(MIMEText(config["SMTP"]["BODY"], 'plain'))

        with open(archivo_salida, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={archivo_salida.name}')
            msg.attach(part)

        with smtplib.SMTP(config["SMTP"]["HOST"], config["SMTP"]["PORT"]) as server:
            server.starttls()
            server.login(config["SMTP"]["USER"], config["SMTP"]["PASSWORD"])
            server.sendmail(config["SMTP"]["USER"], config["SMTP"]["TO"], msg.as_string())

        log_result(f"Correo enviado con archivo {archivo_salida.name} adjunto a {msg['To']}.")
    except FileNotFoundError:
        log_result(f"El archivo {archivo_salida} no existe.")
    except smtplib.SMTPException as e:
        log_result(f"Error SMTP al enviar correo: {e}")
    except Exception as e:
        log_result(f"Error al enviar correo: {e}")
