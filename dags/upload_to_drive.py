"""
Cuarta tarea del DAG: sube output/items_flat.csv (ya generado por
export_csv.py) a una carpeta de Google Drive como una Google Sheet nativa
(no un archivo .csv suelto), sobreescribiendo siempre la misma hoja (mismo
file ID) en vez de acumular una versión por corrida. La conversión CSV ->
Sheet la hace la propia API de Drive al mandar el body con mimeType de
spreadsheet junto al contenido csv (tanto en create como en update) -- no
hace falta ninguna librería de Sheets aparte.

No es un job de Spark: no arma SparkSession ni corre vía SparkSubmitOperator,
por eso vive acá (junto al DAG que lo llama vía PythonOperator) y no en
spark/, que es exclusivamente lo que se manda al cluster (ver CLAUDE.md).
Corre solo dentro del stack de Airflow — no forma parte de `make pipeline`.
"""
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive"]
SHEET_MIME_TYPE = "application/vnd.google-apps.spreadsheet"


def upload_to_drive(csv_path: str, drive_filename: str) -> None:
  # 1. credenciales de la cuenta de servicio (JSON montado de solo lectura,
  #    ver docker-compose.yaml) -- sin login interactivo, igual en cada corrida.
  credentials = service_account.Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
  )
  drive = build("drive", "v3", credentials=credentials, cache_discovery=False)
  folder_id = os.environ["GOOGLE_DRIVE_FOLDER_ID"]

  # 2. buscamos si ya existe un archivo con este nombre en la carpeta destino
  #    (la carpeta tiene que estar compartida como Editor con el client_email
  #    de la cuenta de servicio, o esta búsqueda no la va a ver -- ver CLAUDE.md).
  query = f"name = '{drive_filename}' and '{folder_id}' in parents and trashed = false"
  existing = drive.files().list(q=query, spaces="drive", fields="files(id)").execute()
  matches = existing.get("files", [])

  # 3. mismo archivo (mismo file ID, mismo link) si ya existe -- Drive
  #    reimporta el csv y reemplaza el contenido de la hoja ya nativa; si
  #    es la primera corrida, lo crea directo como Sheet. En los dos casos
  #    el mimeType de destino es el de spreadsheet, no el del csv de origen.
  media = MediaFileUpload(csv_path, mimetype="text/csv", resumable=False)
  if matches:
    file_id = matches[0]["id"]
    drive.files().update(fileId=file_id, body={"mimeType": SHEET_MIME_TYPE}, media_body=media).execute()
  else:
    file_id = (
      drive.files()
      .create(
        body={"name": drive_filename, "parents": [folder_id], "mimeType": SHEET_MIME_TYPE},
        media_body=media,
        fields="id",
      )
      .execute()["id"]
    )

  print(f"Sheet actualizada en Drive -- file_id={file_id}")
