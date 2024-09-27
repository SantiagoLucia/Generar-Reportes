# Generar Reportes

## Descripción

Este proyecto permite generar reportes a partir de consultas SQL y exportarlos a archivos CSV. Además, ofrece la opción de comprimir los archivos CSV generados en un archivo ZIP y enviarlos mediante SFTP o correo electrónico.

## Uso

1. Asegúrate de tener un archivo config.ini con las credenciales de la base de datos y configuración de SFTP y SMTP:

    ```ini
    [ORACLE]
    USER = tu_usuario
    PASSWORD = tu_contraseña
    HOST = tu_host
    PORT = tu_puerto
    SERVICE_NAME = tu_servicio

    [SFTP]
    HOST = tu_sftp_host
    USER = tu_sftp_usuario
    PASSWORD = tu_sftp_contraseña
    DIR = tu_sftp_directorio

    [SMTP]
    HOST = tu_smtp_host
    USER = tu_smtp_usuario
    PASSWORD = tu_smtp_contraseña
    TO = destinatario@correo.com
    ```

2. Ejecuta el script para generar reportes:

    ```sh
    python generar.py --consulta nombre_de_la_consulta.sql
    ```

3. Para generar todos los reportes y comprimirlos en un archivo ZIP:

    ```sh
    python generar.py --consulta all --zip
    ```

4. Para enviar el archivo ZIP generado por SFTP:

    ```sh
    python generar.py --consulta all --zip --sftp
    ```

5. Para enviar el archivo ZIP generado por correo electrónico:

    ```sh
    python generar.py --consulta all --zip --smtp
    ```

## Estructura del Proyecto

- **generar.py**: Script principal para generar reportes, comprimir archivos y enviarlos por SFTP o correo electrónico.
- **config.ini**: Archivo de configuración con las credenciales de la base de datos y configuración de SFTP y SMTP.
- **consultas/**: Directorio que contiene los archivos SQL para las consultas.
- **salida/**: Directorio donde se guardan los archivos CSV generados.
- **reportes.zip**: Archivo ZIP que contiene los archivos CSV comprimidos.
