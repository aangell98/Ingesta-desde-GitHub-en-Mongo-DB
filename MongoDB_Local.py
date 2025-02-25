import requests
import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración desde .env con valores por defecto
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_USER = os.getenv("GITHUB_USER", "microsoft")
GITHUB_PROJECT = os.getenv("GITHUB_PROJECT", "vscode")
START_DATE = os.getenv("START_DATE", "2018-01-01T00:00:00Z")
PER_PAGE = int(os.getenv("PER_PAGE", "100"))

MONGODB_HOST = os.getenv("LOCAL_MONGO_HOST", "localhost")
MONGODB_PORT = int(os.getenv("LOCAL_MONGO_PORT", "27017"))
DB_NAME = os.getenv("LOCAL_MONGO_DB", "github")
COLLECTION_NAME = os.getenv("LOCAL_MONGO_COLLECTION", "commits")

# Verificación de variables críticas
if not GITHUB_TOKEN:
    print("Error: GITHUB_TOKEN no está definido en el archivo .env")
    exit(1)

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Conexión a MongoDB local
try:
    client = MongoClient(MONGODB_HOST, MONGODB_PORT)
    db = client[DB_NAME]
    collection_commits = db[COLLECTION_NAME]
    print("Conexión exitosa a MongoDB local.")
except Exception as e:
    print(f"Error al conectar a MongoDB local: {e}")
    exit(1)

def check_rate_limit(threshold=10):
    rate_url = 'https://api.github.com/rate_limit'
    max_retries = 3
    retries = 0
    
    while retries < max_retries:
        try:
            response = requests.get(rate_url, headers=headers, timeout=10)
            response.raise_for_status()
            rate_limit = response.json()
            remaining = rate_limit['resources']['core']['remaining']
            reset_time = rate_limit['resources']['core']['reset']
            print(f"Peticiones restantes: {remaining}, próximo reset: {time.ctime(reset_time)}")
            if remaining < threshold:
                sleep_time = max(reset_time - int(time.time()) + 5, 0)
                print(f"Esperando {sleep_time} segundos debido al límite de tasas...")
                time.sleep(sleep_time)
            return remaining
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Error al verificar rate limit (Intento {retries}/{max_retries}): {e}")
            if retries < max_retries:
                time.sleep(5)
    
    sleep_time = 60
    print(f"No se pudo verificar el rate limit tras {max_retries} intentos. Esperando {sleep_time} segundos...")
    time.sleep(sleep_time)
    return None

def fetch_with_retries(url, headers, max_retries=3, timeout=10):
    retries = 0
    while retries < max_retries:
        check_rate_limit()
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            status_code = response.status_code
            
            if status_code == 200:
                return response
            elif status_code == 400:
                print(f"Error 400 Bad Request en {url}: solicitud inválida.")
                return None
            elif status_code == 404:
                print(f"Error 404 Resource Not Found en {url}: recurso no encontrado.")
                return None
            elif status_code == 409:
                print(f"Error 409 Conflict en {url}: conflicto en la solicitud.")
                return None
            elif status_code == 500:
                print(f"Error 500 Internal Server Error en {url}: error interno del servidor. Reintentando...")
                retries += 1
                time.sleep(5)
            else:
                print(f"Código de estado inesperado {status_code} en {url}.")
                return None
            
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Error al obtener datos desde {url}. Intento {retries}/{max_retries}: {e}")
            if retries < max_retries:
                time.sleep(5)
    
    print(f"No se pudo obtener datos desde {url} después de {max_retries} intentos.")
    return None

def estimate_total_commits():
    print("Estimando el número total de commits (muestra inicial)...")
    base_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?since={START_DATE}&per_page={PER_PAGE}'
    response = fetch_with_retries(f"{base_url}&page=1", headers)
    if not response or not response.json():
        print("No se pudo obtener datos para la estimación. Asumiendo 1000 commits.")
        return 1000
    
    commits = response.json()
    link_header = response.headers.get('Link', '')
    if 'rel="last"' in link_header:
        for part in link_header.split(','):
            if 'rel="last"' in part:
                total_pages = int(part.split('&page=')[1].split('>')[0])
                break
        total_commits = total_pages * PER_PAGE
        print(f"Estimación basada en encabezado 'Link': {total_commits} commits.")
    else:
        total_commits = len(commits) * 100
        print(f"Estimación aproximada basada en muestra: {total_commits} commits (suponiendo 100 páginas).")
    return total_commits

def get_last_commit_date():
    last_commit = collection_commits.find_one({}, sort=[("commit.committer.date", pymongo.ASCENDING)])
    if last_commit:
        if 'commit' in last_commit and 'committer' in last_commit['commit'] and 'date' in last_commit['commit']['committer']:
            date = last_commit['commit']['committer']['date']
            print(f"Fecha del commit más antiguo encontrado: {date}")
            return date
        else:
            print("Advertencia: El commit más antiguo no tiene el campo 'commit.committer.date'. Continuando sin límite superior.")
    else:
        print("No hay commits previos en la base de datos. Ingestando desde el principio.")
    return None

# Estimación inicial
total_commits_estimate = estimate_total_commits()
ingested_commits = collection_commits.count_documents({"projectId": GITHUB_PROJECT})
print(f"Commits ya ingestados: {ingested_commits} de un estimado de {total_commits_estimate}")

# Obtener la fecha del commit más antiguo
last_commit_date = get_last_commit_date()
if last_commit_date:
    print(f"Continuando desde el commit más antiguo: {last_commit_date}")
    until_date = last_commit_date
else:
    print(f"Ingestando desde {START_DATE} sin límite superior.")
    until_date = None

page = 1
has_new_commits = True

try:
    while has_new_commits:
        if until_date:
            search_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?page={page}&per_page={PER_PAGE}&since={START_DATE}&until={until_date}'
        else:
            search_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?page={page}&per_page={PER_PAGE}&since={START_DATE}'
        
        response = fetch_with_retries(search_url, headers)
        
        if not response or not response.json():
            print("No se encontraron más commits o ocurrió un error.")
            break

        commits = response.json()
        if not commits:
            print("No hay más commits disponibles en este rango.")
            break

        has_new_commits = False
        for commit in commits:
            commit_sha = commit['sha']
            commit_url = commit['url']

            if collection_commits.find_one({"sha": commit_sha}):
                print(f"Commit {commit_sha} ya existe en MongoDB, omitiendo...")
                continue

            commit_response = fetch_with_retries(commit_url, headers)
            if not commit_response:
                print(f"No se pudieron obtener detalles del commit {commit_sha}. Omitiendo...")
                continue

            commit_data = commit_response.json()
            commit_data['files_modified'] = commit_data.get('files', [])
            commit_data['stats'] = commit_data.get('stats', [])
            commit_data['projectId'] = GITHUB_PROJECT

            try:
                commit_date = commit_data['commit']['committer']['date']
                collection_commits.insert_one(commit_data)
                ingested_commits += 1
                has_new_commits = True
                print(f"Commit {commit_sha} insertado en MongoDB. Fecha: {commit_date}. Progreso: {ingested_commits}/{total_commits_estimate}")
            except Exception as e:
                print(f"Error al insertar commit {commit_sha}: {e}")

        page += 1

except KeyboardInterrupt:
    print("\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
    print(f"Commits ingestados hasta el momento: {ingested_commits} de un estimado de {total_commits_estimate}")
    print("Hasta pronto!")
    exit(0)

print("Proceso completado.")