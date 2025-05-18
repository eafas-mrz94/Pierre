import os
import io
import pandas as pd
import datetime
import psycopg2
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from rapidfuzz import process

FOLDER_ID = "1K4Vgc-pyLuMcJUJ_Wel2hNCoUnDomTim"
DATABASE_URL = "postgresql://neondb_owner:npg_fGCgBXD0EkR8@ep-mute-tooth-a20kp0rx-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"
SERVICE_ACCOUNT_FILE = "pierre-pleignet.json"
DOWNLOAD_DIR = "telechargements_csv"

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

results = drive_service.files().list(
    q=f"'{FOLDER_ID}' in parents and mimeType='text/csv'",
    fields="files(id, name)"
).execute()

files = results.get("files", [])
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

for file in files:
    file_id = file["id"]
    file_name = file["name"]
    print(f"📥 Fichier détecté : {file_name}")

    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    cleaned_name = file_name.replace(" ", "_").replace("(", "").replace(")", "")
    local_path = os.path.join(DOWNLOAD_DIR, cleaned_name)
    with open(local_path, "wb") as f:
        f.write(fh.getvalue())

    print(f"✅ Téléchargé : {cleaned_name}")

    try:
        try:
            df = pd.read_csv(local_path, sep=";", encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(local_path, sep=";", encoding="ISO-8859-1")
    except Exception as e:
        print(f"❌ Erreur de lecture : {e}")
        continue

    print(f"📊 Colonnes détectées : {list(df.columns)}")

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    entreprise = file_name.split("_")[0]
    type_fichier = file_name.split("_")[1] if "_" in file_name else "inconnu"
    date_import = datetime.datetime.now()

    try:
        for i, row in df.iterrows():
            for col in df.columns:
                cursor.execute(
                    "INSERT INTO donnees_briques (entreprise, type_fichier, ligne, nom_colonne, valeur, date_import) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        entreprise,
                        type_fichier,
                        i + 1,
                        col,
                        str(row[col]),
                        date_import
                    )
                )
        conn.commit()
        print(f"✅ Insertion PostgreSQL OK")
    except Exception as e:
        print(f"❌ Erreur PostgreSQL : {e}")
        conn.rollback()

cursor.close()
conn.close()
