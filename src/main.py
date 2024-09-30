import multiprocessing as mp
from pathlib import Path
from config import cargar_config
from report_generator import generar_reporte
from file_utils import comprimir, eliminar_archivos_csv
from sftp_utils import enviar_sftp
from email_utils import enviar_email_con_adjunto
from logger import log_result
import inquirer
import tqdm

config = cargar_config()

def obtener_args() -> dict:
    """
    Obtiene las consultas SQL y los protocolos de envío seleccionados por el usuario.

    Returns:
        dict: Diccionario con las consultas y protocolos seleccionados.
    """
    consultas_path = Path(config["RUTAS"]["CONSULTAS"])
    sql_files = list(consultas_path.glob("*.sql"))
    
    if not sql_files:
        raise FileNotFoundError(f"No se encontraron archivos .sql en el directorio {config["RUTAS"]["CONSULTAS"]}")

    questions = [
        inquirer.Checkbox(
            "consultas",
            message="Seleccione las consultas a ejecutar (<espacio> para seleccionar, <ctrl+a> para seleccionar todo)",
            choices=[(path.name, path) for path in sql_files],
        )
    ]
    answers = inquirer.prompt(questions)
    path_consultas = answers["consultas"]

    questions = [
        inquirer.Checkbox(
            "Protocolos",
            message="Seleccione los protocolos de envío (<espacio> para seleccionar, <ctrl+a> para seleccionar todo)",
            choices=["SFTP", "SMTP"],
        )
    ]
    answers = inquirer.prompt(questions)
    modos_envio = answers["Protocolos"]

    if not path_consultas:
        raise ValueError("Debe seleccionar al menos una consulta.")

    return {"consultas": path_consultas, "protocolos": modos_envio}


def main() -> None:

    args = obtener_args()

    try:
        processes = int(config["GENERAL"]["PROCESSES"])
        with mp.Pool(processes=processes) as pool:
            processes = [
                pool.apply_async(generar_reporte, args=(path,), callback=log_result)
                for path in args["consultas"]
            ]
            
            for process in tqdm.tqdm(processes, desc="Generando reportes", total=len(processes)):
                process.get()

        comprimir()
        eliminar_archivos_csv()

        if "SFTP" in args["protocolos"]:
            enviar_sftp()
        
        if "SMTP" in args["protocolos"]:
            enviar_email_con_adjunto()

    except KeyError as e:
        log_result(f"Clave de configuración faltante: {e}")
    except ValueError as e:
        log_result(f"Valor inválido: {e}")
    except Exception as e:
        log_result(f"Error durante la ejecución: {e}")

if __name__ == "__main__":
    main()