import os
import io
import re
import pandas as pd
import psycopg2
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# === CONFIGURATION ===
SERVICE_ACCOUNT_FILE = 'pierre-pleignet.json'
FOLDER_ID = '1K4Vgc-pyLuMcJUJ_Wel2hNCoUnDomTim'
DOWNLOAD_DIR = 'telechargements_csv'

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'Pierre2025',
    'host': 'db.ynximbxkdbangzqlxhly.supabase.co',
    'port': '5432'
}

# === SETUP ===
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

# === PARSE NOM FICHIER
def parse_filename(file_name):
    match = re.match(r"(\w+?)_(\w+?)_\d{6}", file_name)
    if match:
        return match.group(1).lower(), match.group(2).lower()
    return "inconnu", "inconnu"

# === INSERTION DANS SUPABASE
def insert_cells(df, entreprise, fichier_type, conn):
    now = datetime.now()
    with conn.cursor() as cur:
        for i, row in df.iterrows():
            for col in df.columns:
                valeur = row[col]
                cur.execute("""
                    INSERT INTO donnees_briques (entreprise, type_fichier, ligne, nom_colonne, valeur, date_import)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (entreprise, fichier_type, i + 1, col, str(valeur), now))
    conn.commit()

# === LECTURE DES FICHIERS SUR DRIVE
results = service.files().list(
    q=f"'{FOLDER_ID}' in parents and mimeType='text/csv'",
    fields="files(id, name)",
).execute()

files = results.get('files', [])
for file in files:
    file_id = file['id']
    file_name = file['name']
    entreprise, fichier_type = parse_filename(file_name)

    request = service.files().get_media(fileId=file_id)
    local_path = os.path.join(DOWNLOAD_DIR, file_name.replace(" ", "_"))
    with open(local_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    print(f"✅ Téléchargé : {file_name}")
    try:
        df = pd.read_csv(local_path, sep=';', encoding='ISO-8859-1')
        print("📊 Colonnes détectées :", df.columns.tolist())
    except Exception as e:
        print(f"❌ Erreur lecture CSV : {e}")
        continue

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            insert_cells(df, entreprise, fichier_type, conn)
        print(f"✅ Insertion terminée pour {entreprise} ({fichier_type})")
    except Exception as e:
        print(f"❌ Erreur PostgreSQL : {e}")
