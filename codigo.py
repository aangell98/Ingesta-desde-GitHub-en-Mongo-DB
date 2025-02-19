import requests
import pymongo
from pymongo import MongoClient
import time

# Datos de conexión a MongoDB Atlas
MONGODB_URI = "mongodb+srv://<username>:<password>@<cluster-url>/test?retryWrites=true&w=majority"
DB_NAME = 'github'
COLLECTION_COMMITS = 'commits'

# Datos de autenticación para GitHub
token = '<tu-token-de-github>'
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
    response = requests.get(rate_url, headers=headers)
    rate_limit = response.json()
    remaining = rate_limit['resources']['core']['remaining']
    reset_time = rate_limit['resources']['core']['reset']
    if remaining < 10:
        sleep_time = reset_time - int(time.time()) + 10
        print(f"Esperando {sleep_time} segundos debido al límite de tasas...")
        time.sleep(sleep_time)

# Conexión a MongoDB
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
collection_commits = db[COLLECTION_COMMITS]

# Bucle para obtener todos los commits
while True:
    search_url = f'https://api.github.com/search/commits?q=repo:{user}/{project}+committer-date:>={start_date}&sort=committer-date&order=asc&page={page}&per_page={per_page}'
    check_rate_limit()
    r = requests.get(search_url, headers=headers)
    commits_dict = r.json()
    
    if 'items' not in commits_dict:
        print("Error al obtener commits:", commits_dict)
        break
    
    commits = commits_dict['items']
    
    if not commits:
        break
    
    for commit in commits:
        commit_sha = commit['sha']
        commit_url = commit['url']
        
        # Obtener detalles del commit
        check_rate_limit()
        commit_details = requests.get(commit_url, headers=headers).json()
        
        # Extraer información extendida
        files_modified = commit_details.get('files', [])
        stats = commit_details.get('stats', {})
        
        # Agregar campos nuevos al documento
        commit['files_modified'] = files_modified
        commit['stats'] = stats
        commit['projectId'] = project
        
        # Insertar en MongoDB
        collection_commits.insert_one(commit)
    
    page += 1

print("Proceso completado.")