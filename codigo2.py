import requests
import pymongo
from pymongo import MongoClient
import time

# Datos de conexión a MongoDB Atlas
MONGODB_URI = "mongodb+srv://:@gestiondatos.rrewd.mongodb.net/github?retryWrites=true&w=majority" #cambiar user y psw

# Datos de autenticación para GitHub
token = '' #ingresar token de autenticación
headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

# Parámetros de consulta
user = 'microsoft'
project = 'vscode'
start_date = '2018-01-01'
page = 1
per_page = 100

# Función para gestionar el rate limit
def check_rate_limit():
    rate_url = 'https://api.github.com/rate_limit'
    try:
        response = requests.get(rate_url, headers=headers, timeout=10)
        rate_limit = response.json()
        remaining = rate_limit['resources']['core']['remaining']
        reset_time = rate_limit['resources']['core']['reset']
        print(f"Peticiones restantes: {remaining}, próximo reset: {time.ctime(reset_time)}")
        if remaining < 10:
            sleep_time = reset_time - int(time.time()) + 10
            print(f"Esperando {sleep_time} segundos debido al límite de tasas...")
            time.sleep(sleep_time)
    except Exception as e:
        print(f"Error al verificar el rate limit: {e}")

# Función para realizar solicitudes con reintentos
def fetch_with_retries(url, headers, max_retries=3, timeout=10):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()  # Lanza una excepción si hay un error HTTP
            return response.json()
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Error al obtener datos desde {url}. Intento {retries}/{max_retries}: {e}")
            if retries < max_retries:
                time.sleep(5)  # Espera antes de reintentar
    print(f"No se pudo obtener datos desde {url} después de {max_retries} intentos.")
    return None

# Conexión a MongoDB
try:
    client = MongoClient(MONGODB_URI)
    db = client['github']
    collection_commits = db['commits']
    print("Conexión exitosa a MongoDB.")
except Exception as e:
    print(f"Error al conectar a MongoDB: {e}")
    exit(1)

# Bucle para obtener todos los commits
while True:
    search_url = f'https://api.github.com/search/commits?q=repo:{user}/{project}+committer-date:>={start_date}&sort=committer-date&order=asc&page={page}&per_page={per_page}'
    check_rate_limit()
    commits_dict = fetch_with_retries(search_url, headers)
    
    if not commits_dict or 'items' not in commits_dict:
        print("No se encontraron más commits o ocurrió un error.")
        break

    commits = commits_dict.get('items', [])
    if not commits:
        print("No hay más commits disponibles.")
        break

    for commit in commits:
        commit_sha = commit['sha']
        commit_url = commit['url']

        # Obtener detalles del commit
        check_rate_limit()
        commit_details = fetch_with_retries(commit_url, headers)
        if commit_details is None:
            print(f"No se pudieron obtener detalles del commit {commit_sha}. Omitiendo...")
            continue

        # Extraer información extendida
        files_modified = commit_details.get('files', [])
        stats = commit_details.get('stats', {})

        # Agregar campos nuevos al documento
        commit['files_modified'] = files_modified
        commit['stats'] = stats
        commit['projectId'] = project

        # Insertar en MongoDB (evitar duplicados)
        if not collection_commits.find_one({"sha": commit_sha}):
            try:
                collection_commits.insert_one(commit)
                print(f"Commit {commit_sha} insertado en MongoDB.")
            except Exception as e:
                print(f"Error al insertar commit {commit_sha}: {e}")
        else:
            print(f"Commit {commit_sha} ya existe en MongoDB, omitiendo...")

    page += 1

print("Proceso completado.")