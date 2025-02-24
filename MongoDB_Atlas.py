import requests
import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timezone

# Datos de conexión a MongoDB Atlas
MONGODB_URI = "mongodb+srv://user:password@gestiondatos.rrewd.mongodb.net/github?retryWrites=true&w=majority"

# Datos de autenticación para GitHub
token = 'token'
headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

# Parámetros de consulta
user = 'microsoft'
project = 'vscode'
start_date = '2018-01-01T00:00:00Z'  # Formato ISO 8601
per_page = 100  # Máximo permitido por la API

# Conexión a MongoDB
try:
    client = MongoClient(MONGODB_URI)
    db = client['github']
    collection_commits = db['commits']
    print("Conexión exitosa a MongoDB.")
except Exception as e:
    print(f"Error al conectar a MongoDB: {e}")
    exit(1)

# Función para gestionar el rate limit con reintentos
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
    
    # Si falla después de 3 intentos, esperar un tiempo fijo
    sleep_time = 60  # Valor por defecto en segundos
    print(f"No se pudo verificar el rate limit tras {max_retries} intentos. Esperando {sleep_time} segundos...")
    time.sleep(sleep_time)
    return None

# Función genérica para realizar solicitudes con reintentos
def fetch_with_retries(url, headers, max_retries=3, timeout=10):
    retries = 0
    while retries < max_retries:
        check_rate_limit()
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Error al obtener datos desde {url}. Intento {retries}/{max_retries}: {e}")
            if retries < max_retries:
                time.sleep(5)
    print(f"No se pudo obtener datos desde {url} después de {max_retries} intentos.")
    return None

# Estimación aproximada del total de commits
def estimate_total_commits():
    print("Estimando el número total de commits (muestra inicial)...")
    base_url = f'https://api.github.com/repos/{user}/{project}/commits?since={start_date}&per_page={per_page}'
    
    # Obtener la primera página para la muestra
    response = fetch_with_retries(f"{base_url}&page=1", headers)
    if not response or not response.json():
        print("No se pudo obtener datos para la estimación. Asumiendo 1000 commits.")
        return 1000  # Valor por defecto en caso de fallo
    
    commits = response.json()
    commits_in_page = len(commits)
    
    # Intentar obtener el número total de páginas desde el encabezado 'Link'
    link_header = response.headers.get('Link', '')
    total_pages = None
    if 'rel="last"' in link_header:
        for part in link_header.split(','):
            if 'rel="last"' in part:
                total_pages = int(part.split('&page=')[1].split('>')[0])
                break
    
    if total_pages:
        total_commits = total_pages * per_page
        print(f"Estimación basada en encabezado 'Link': {total_commits} commits.")
    else:
        total_commits = commits_in_page * 100  # Extrapolación aproximada
        print(f"Estimación aproximada basada en muestra: {total_commits} commits (suponiendo 100 páginas).")
    
    return total_commits

# Obtener la fecha del último commit insertado
def get_last_commit_date():
    last_commit = collection_commits.find_one({}, sort=[("commit.committer.date", -1)])
    if last_commit and 'commit' in last_commit and 'committer' in last_commit['commit']:
        return last_commit['commit']['committer']['date']
    return None

# Proceso principal
total_commits_estimate = estimate_total_commits()
ingested_commits = collection_commits.count_documents({"projectId": project})
print(f"Commits ya ingestados: {ingested_commits} de un estimado de {total_commits_estimate}")

last_commit_date = get_last_commit_date()
if last_commit_date:
    print(f"Último commit insertado: {last_commit_date}. Ingestando commits posteriores...")
else:
    print(f"No hay commits previos en la base de datos. Ingestando desde {start_date}...")
    last_commit_date = start_date

page = 1

try:
    while True:
        # Usar 'since' y 'until' para limitar el rango de commits
        search_url = f'https://api.github.com/repos/{user}/{project}/commits?page={page}&per_page={per_page}&since={start_date}&until={last_commit_date}'
        response = fetch_with_retries(search_url, headers)
        
        if not response or not response.json():
            print("No se encontraron más commits o ocurrió un error.")
            break

        commits = response.json()
        if not commits:
            print("No hay más commits disponibles.")
            break

        for commit in commits:
            commit_sha = commit['sha']
            commit_url = commit['url']

            # Verificar si el commit ya existe
            if collection_commits.find_one({"sha": commit_sha}):
                print(f"Commit {commit_sha} ya existe en MongoDB, omitiendo...")
                continue

            # Obtener detalles del commit
            commit_response = fetch_with_retries(commit_url, headers)
            if not commit_response:
                print(f"No se pudieron obtener detalles del commit {commit_sha}. Omitiendo...")
                continue

            commit_data = commit_response.json()
            files_modified = commit_data.get('files', [])
            stats = commit_data.get('stats', {})

            # Agregar campos nuevos al documento
            commit['files_modified'] = files_modified
            commit['stats'] = stats
            commit['projectId'] = project

            # Insertar en MongoDB
            try:
                collection_commits.insert_one(commit)
                ingested_commits += 1
                print(f"Commit {commit_sha} insertado en MongoDB. Progreso: {ingested_commits}/{total_commits_estimate}")
            except Exception as e:
                print(f"Error al insertar commit {commit_sha}: {e}")

        page += 1

except KeyboardInterrupt:
    print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
    print(f"Commits ingestados hasta el momento: {ingested_commits} de un estimado de {total_commits_estimate}")
    print("Hasta pronto!")
    exit(0)

print("Proceso completado.")