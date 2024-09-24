# Generar Reportes

## Descripción

Este proyecto permite generar reportes a partir de consultas SQL y exportarlos a archivos CSV. Además, ofrece la opción de comprimir los archivos CSV generados en un archivo ZIP.

## Requisitos

- Python 3.7 o superior
- Módulos de Python:
  - `sqlalchemy`
  - `multiprocessing`
  - `argparse`
  - `pandas`
  - `pathlib`
  - `zipfile`
  - `configparser`

## Uso

1. Asegúrate de tener un archivo `config.ini` con las credenciales de la base de datos:

   ```ini
   [ORACLE]
   USER = tu_usuario
   PASSWORD = tu_contraseña
   HOST = tu_host
   PORT = tu_puerto
   SERVICE_NAME = tu_servicio
   ```

2. Ejecuta el script para generar reportes:

   ```sh
   python generar.py --consulta nombre_de_la_consulta.sql
   ```

3. Para generar todos los reportes y comprimirlos en un archivo ZIP:

   ```sh
   python generar.py --consulta all --zip
   ```

## Estructura del Proyecto

- `generar.py`: Script principal para generar reportes y comprimir archivos.
- `config.ini`: Archivo de configuración con las credenciales de la base de datos.
- `consultas/`: Directorio que contiene los archivos SQL para las consultas.
- `salida/`: Directorio donde se guardan los archivos CSV generados.
- `reportes.zip`: Archivo ZIP que contiene los archivos CSV comprimidos.
