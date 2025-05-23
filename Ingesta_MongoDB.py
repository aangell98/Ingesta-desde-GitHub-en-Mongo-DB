import requests
import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración desde .env con valores por defecto
GITHUB_TOKENS = os.getenv("GITHUB_TOKENS", os.getenv("GITHUB_TOKEN")).split(",")  # Lista de tokens, fallback a GITHUB_TOKEN
GITHUB_USER = os.getenv("GITHUB_USER", "microsoft")
GITHUB_PROJECT = os.getenv("GITHUB_PROJECT", "vscode")
START_DATE = os.getenv("START_DATE", "2018-01-01T00:00:00Z")
PER_PAGE = int(os.getenv("PER_PAGE", "100"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

MONGODB_HOST = os.getenv("LOCAL_MONGO_HOST", "localhost")
MONGODB_PORT = int(os.getenv("LOCAL_MONGO_PORT", "27017"))
MONGODB_URI = os.getenv("ATLAS_MONGO_URI")
DB_NAME = os.getenv("LOCAL_MONGO_DB", "github")
COLLECTION_NAME = os.getenv("LOCAL_MONGO_COLLECTION", "commits")

# Archivo para guardar el tiempo acumulado
TIME_FILE = "ingestion_time.json"

# Verificación de variables críticas
if not GITHUB_TOKENS or not GITHUB_TOKENS[0]:
    print("Error: GITHUB_TOKENS o GITHUB_TOKEN no está definido en el archivo .env")
    exit(1)

# Índice del token actual y encabezados iniciales
current_token_index = 0
headers = {
    "Authorization": f"token {GITHUB_TOKENS[current_token_index]}",
    "Accept": "application/vnd.github.v3+json"
}

# Selección de conexión a MongoDB
def connect_to_mongodb():
    print("\n=== Selección de Base de Datos ===")
    print("1. Conectar a MongoDB Local")
    print("2. Conectar a MongoDB Atlas")
    choice = input("Selecciona una opción (1-2): ")

    if choice == "1":
        try:
            client = MongoClient(MONGODB_HOST, MONGODB_PORT)
            print("Conexión exitosa a MongoDB Local.")
            return client
        except Exception as e:
            print(f"Error al conectar a MongoDB Local: {e}")
            exit(1)
    elif choice == "2":
        if not MONGODB_URI:
            print("Error: ATLAS_MONGO_URI no está definido en el archivo .env")
            exit(1)
        try:
            client = MongoClient(MONGODB_URI)
            print("Conexión exitosa a MongoDB Atlas.")
            return client
        except Exception as e:
            print(f"Error al conectar a MongoDB Atlas: {e}")
            exit(1)
    else:
        print("Opción inválida. Saliendo del programa.")
        exit(1)

# Conexión global
client = connect_to_mongodb()
db = client[DB_NAME]
collection_commits = db[COLLECTION_NAME]

# Contador para mensajes de rate limit
request_count = 0

def check_rate_limit(threshold=100):
    global request_count, current_token_index, headers
    rate_url = 'https://api.github.com/rate_limit'
    max_retries = 3
    retries = 0
    
    while retries < max_retries:
        try:
            response = requests.get(rate_url, headers=headers, timeout=30)
            response.raise_for_status()
            rate_limit = response.json()
            remaining = rate_limit['resources']['core']['remaining']
            reset_time = rate_limit['resources']['core']['reset']
            request_count += 1
            if request_count % 10 == 0 or remaining < threshold:
                print(f"Peticiones restantes con token actual ({GITHUB_TOKENS[current_token_index][:8]}...): {remaining}, próximo reset: {time.ctime(reset_time)}")
            if remaining < threshold:
                if len(GITHUB_TOKENS) > 1:  # Si hay más de un token
                    current_token_index = (current_token_index + 1) % len(GITHUB_TOKENS)
                    headers["Authorization"] = f"token {GITHUB_TOKENS[current_token_index]}"
                    print(f"El límite de tasa se ha agotado. Cambiando automáticamente al token {GITHUB_TOKENS[current_token_index][:8]}...")
                    return check_rate_limit(threshold)  # Reintentar con el nuevo token
                # Si solo hay un token, esperar
                sleep_time = max(reset_time - int(time.time()) + 5, 0)
                hours, remainder = divmod(sleep_time, 3600)
                minutes, seconds = divmod(remainder, 60)
                print(f"Esperando {hours} horas, {minutes} minutos y {seconds} segundos debido al límite de tasas. \nLa ingesta se reanudará automáticamente después de {time.ctime(reset_time)}")
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

def fetch_with_retries(url, headers=headers, max_retries=3, timeout=30):
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
                time.sleep(2 ** retries)  # Backoff exponencial: 2, 4, 8 segundos
            else:
                print(f"Código de estado inesperado {status_code} en {url}.")
                return None
            
        except requests.exceptions.ConnectTimeout as e:
            retries += 1
            print(f"Timeout de conexión en {url}. Intento {retries}/{max_retries}: {e}")
            if retries < max_retries:
                time.sleep(2 ** retries)  # Backoff exponencial
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Error al obtener datos desde {url}. Intento {retries}/{max_retries}: {e}")
            if retries < max_retries:
                time.sleep(2 ** retries)  # Backoff exponencial
  
    print(f"No se pudo obtener datos desde {url} después de {max_retries} intentos.")
    return None

def fetch_commit_details(commit):
    commit_sha = commit['sha']
    commit_url = commit['url']
    response = fetch_with_retries(commit_url, headers)
    if not response:
        print(f"No se pudieron obtener detalles del commit {commit_sha}. Omitiendo...")
        return None
    commit_data = response.json()
    commit_data['files_modified'] = commit_data.get('files', [])
    commit_data['stats'] = commit_data.get('stats', [])
    commit_data['projectId'] = GITHUB_PROJECT
    return commit_data

def estimate_total_commits(start_date=START_DATE):
    print("Estimando el número total de commits (muestra inicial)...")
    base_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?since={start_date}&per_page={PER_PAGE}'
    response = fetch_with_retries(f"{base_url}&page=1", headers)
    if not response or not response.json():
        print("No se pudo obtener datos para la estimación. Asumiendo 1000 commits.")
        return 1000
    
    commits = response.json()
    commits_in_page = len(commits)
    link_header = response.headers.get('Link', '')
    if 'rel="last"' in link_header:
        for part in link_header.split(','):
            if 'rel="last"' in part:
                total_pages = int(part.split('&page=')[1].split('>')[0])
                break
        total_commits = total_pages * PER_PAGE
        print(f"Estimación basada en encabezado 'Link': {total_commits} commits.")
    else:
        total_commits = commits_in_page * 100
        print(f"Estimación aproximada basada en muestra: {total_commits} commits (suponiendo 100 páginas).")
    return total_commits

def get_last_commit_date():
    last_commit = collection_commits.find_one({}, sort=[("commit.committer.date", pymongo.ASCENDING)])
    if last_commit and 'commit' in last_commit and 'committer' in last_commit['commit'] and 'date' in last_commit['commit']['committer']:
        return last_commit['commit']['committer']['date']
    return None

def get_newest_commit_date():
    newest_commit = collection_commits.find_one({}, sort=[("commit.committer.date", pymongo.DESCENDING)])
    if newest_commit and 'commit' in newest_commit and 'committer' in newest_commit['commit'] and 'date' in newest_commit['commit']['committer']:
        return newest_commit['commit']['committer']['date']
    return None

def get_newest_date_before_oldest(oldest_date):
    newest_before_oldest = collection_commits.find_one(
        {"commit.committer.date": {"$lt": oldest_date}},
        sort=[("commit.committer.date", pymongo.DESCENDING)]
    )
    if newest_before_oldest and 'commit' in newest_before_oldest and 'committer' in newest_before_oldest['commit'] and 'date' in newest_before_oldest['commit']['committer']:
        return newest_before_oldest['commit']['committer']['date']
    return None

def load_previous_time():
    if os.path.exists(TIME_FILE):
        with open(TIME_FILE, 'r') as f:
            data = json.load(f)
            return data.get("elapsed_time", 0)
    return 0

def save_time(elapsed_time):
    with open(TIME_FILE, 'w') as f:
        json.dump({"elapsed_time": elapsed_time}, f)

def delete_time_file():
    if os.path.exists(TIME_FILE):
        os.remove(TIME_FILE)

def ingest_first_time(start_time):
    previous_time = load_previous_time()
    total_commits_estimate = estimate_total_commits()
    ingested_commits = collection_commits.count_documents({"projectId": GITHUB_PROJECT})
    print(f"Commits ya ingestados: {ingested_commits} de un estimado de {total_commits_estimate}")

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
            print(f"Página {page}: Encontrados {len(commits)} commits en la respuesta de la API")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                commits_to_fetch = [commit for commit in commits if not collection_commits.find_one({"sha": commit['sha']})]
                if commits_to_fetch:
                    future_to_commit = {executor.submit(fetch_commit_details, commit): commit for commit in commits_to_fetch}
                    for future in as_completed(future_to_commit):
                        commit_data = future.result()
                        if commit_data:
                            commit_sha = commit_data['sha']
                            commit_date = commit_data['commit']['committer']['date']
                            try:
                                collection_commits.insert_one(commit_data)
                                ingested_commits += 1
                                has_new_commits = True
                                print(f"Commit {commit_sha} insertado en MongoDB. Fecha: {commit_date}. Progreso: {ingested_commits}/{total_commits_estimate}")
                            except Exception as e:
                                print(f"Error al insertar commit {commit_sha}: {e}")

            page += 1

    except KeyboardInterrupt:
        elapsed_time = previous_time + (time.time() - start_time)
        save_time(elapsed_time)
        print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
        hours, remainder = divmod(int(elapsed_time), 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Tiempo de ejecución acumulado hasta la interrupción: {hours} horas, {minutes} minutos y {seconds} segundos")
        print(f"Commits ingestados hasta el momento: {ingested_commits} de un estimado de {total_commits_estimate}")
        print("Hasta pronto!")
        exit(0)

    elapsed_time = previous_time + (time.time() - start_time)
    delete_time_file()
    hours, remainder = divmod(int(elapsed_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Proceso completado en {hours} horas, {minutes} minutos y {seconds} segundos")

def ingest_new_commits(start_time):
    previous_time = load_previous_time()
    ingested_commits_before = collection_commits.count_documents({"projectId": GITHUB_PROJECT})
    print(f"Commits actualmente en la base de datos: {ingested_commits_before}")

    newest_date = get_newest_commit_date()
    if not newest_date:
        print("No hay commits previos en la base de datos. Por favor, ejecuta la opción 1 primero.")
        return

    print(f"Buscando nuevos commits desde el más reciente: {newest_date}")
    print("ADVERTENCIA: Si es la primera vez que ejecutas el programa o la ingesta inicial no fue completada, podrías corromper los datos existentes en la base de datos.")
    confirmation = input("¿Estás seguro de que quieres continuar? (si/no): ")
    if confirmation.lower() != "si":
        print("Operación cancelada.")
        return

    page = 1
    has_new_commits = True
    new_commits_count = 0

    try:
        while has_new_commits:
            search_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?page={page}&per_page={PER_PAGE}&since={newest_date}'
            response = fetch_with_retries(search_url, headers)
            
            if not response or not response.json():
                print("No se encontraron más commits nuevos o ocurrió un error.")
                break

            commits = response.json()
            if not commits:
                print("No hay más commits nuevos disponibles.")
                break

            has_new_commits = False
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                commits_to_fetch = [commit for commit in commits if not collection_commits.find_one({"sha": commit['sha']})]
                if commits_to_fetch:
                    future_to_commit = {executor.submit(fetch_commit_details, commit): commit for commit in commits_to_fetch}
                    for future in as_completed(future_to_commit):
                        commit_data = future.result()
                        if commit_data:
                            commit_sha = commit_data['sha']
                            commit_date = commit_data['commit']['committer']['date']
                            try:
                                collection_commits.insert_one(commit_data)
                                ingested_commits_before += 1
                                new_commits_count += 1
                                has_new_commits = True
                                print(f"Commit {commit_sha} insertado en MongoDB (nuevo). Fecha: {commit_date}")
                            except Exception as e:
                                print(f"Error al insertar commit {commit_sha}: {e}")
                else:
                    print("Se encontró un commit duplicado. Finalizando la ingesta de nuevos commits.")
                    has_new_commits = False
                    break

            page += 1

    except KeyboardInterrupt:
        elapsed_time = previous_time + (time.time() - start_time)
        save_time(elapsed_time)
        print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
        hours, remainder = divmod(int(elapsed_time), 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Tiempo de ejecución acumulado hasta la interrupción: {hours} horas, {minutes} minutos y {seconds} segundos")
        print(f"Commits nuevos ingestados en esta ejecución: {new_commits_count}")
        print(f"Nuevo total de commits en la base de datos: {ingested_commits_before}")
        print("Hasta pronto!")
        exit(0)

    elapsed_time = previous_time + (time.time() - start_time)
    delete_time_file()
    hours, remainder = divmod(int(elapsed_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Ingesta de nuevos commits completada en {hours} horas, {minutes} minutos y {seconds} segundos")

def ingest_older_commits(start_time):
    previous_time = load_previous_time()
    ingested_commits = collection_commits.count_documents({"projectId": GITHUB_PROJECT})
    print(f"Commits actualmente en la base de datos: {ingested_commits}")

    oldest_date = get_last_commit_date()
    if not oldest_date:
        print("No hay commits previos en la base de datos. Por favor, ejecuta la opción 1 primero.")
        return

    print(f"Fecha del commit más antiguo actual: {oldest_date}")
    print("Por favor, introduce la fecha hasta la cual deseas ampliar la ingesta (formato: YYYY-MM-DDTHH:MM:SSZ, ej. 2017-01-01T00:00:00Z):")
    new_start_date = input("Nueva fecha de inicio: ")
    try:
        datetime.strptime(new_start_date, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        print("Formato de fecha inválido. Usa YYYY-MM-DDTHH:MM:SSZ (ej. 2017-01-01T00:00:00Z).")
        return

    new_start_date_dt = datetime.strptime(new_start_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    oldest_date_dt = datetime.strptime(oldest_date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    if new_start_date_dt >= oldest_date_dt:
        print(f"La nueva fecha de inicio ({new_start_date}) debe ser anterior al commit más antiguo actual ({oldest_date}).")
        return

    newest_before_oldest = get_newest_date_before_oldest(oldest_date)
    if newest_before_oldest:
        print(f"Continuando desde el commit más reciente antes de {oldest_date}: {newest_before_oldest}")
        since_date = newest_before_oldest
    else:
        print(f"No hay commits previos a {oldest_date} en la base de datos. Iniciando desde {new_start_date}")
        since_date = new_start_date

    print(f"Ampliando ingesta desde {since_date} hasta {oldest_date}")
    page = 1
    has_new_commits = True
    new_commits_count = 0

    try:
        while has_new_commits:
            search_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?page={page}&per_page={PER_PAGE}&since={since_date}&until={oldest_date}'
            response = fetch_with_retries(search_url, headers)
            
            if not response or not response.json():
                print(f"No se encontraron más commits en el rango {since_date} a {oldest_date} o ocurrió un error.")
                break

            commits = response.json()
            print(f"Página {page}: Encontrados {len(commits)} commits en la respuesta de la API")
            if not commits:
                print("No hay más commits disponibles en este rango.")
                break

            has_new_commits = False
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                commits_to_fetch = [commit for commit in commits if not collection_commits.find_one({"sha": commit['sha']})]
                print(f"Página {page}: {len(commits_to_fetch)} commits nuevos para procesar")
                if commits_to_fetch:
                    future_to_commit = {executor.submit(fetch_commit_details, commit): commit for commit in commits_to_fetch}
                    for future in as_completed(future_to_commit):
                        commit_data = future.result()
                        if commit_data:
                            commit_sha = commit_data['sha']
                            commit_date = commit_data['commit']['committer']['date']
                            try:
                                collection_commits.insert_one(commit_data)
                                ingested_commits += 1
                                new_commits_count += 1
                                has_new_commits = True
                                print(f"Commit {commit_sha} insertado en MongoDB (Antiguo). Fecha: {commit_date}")
                            except Exception as e:
                                print(f"Error al insertar commit {commit_sha}: {e}")

            page += 1

    except KeyboardInterrupt:
        elapsed_time = previous_time + (time.time() - start_time)
        save_time(elapsed_time)
        print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
        hours, remainder = divmod(int(elapsed_time), 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Tiempo de ejecución acumulado hasta la interrupción: {hours} horas, {minutes} minutos y {seconds} segundos")
        print(f"Commits nuevos ingestados en esta ejecución: {new_commits_count}")
        print(f"Nuevo total de commits en la base de datos: {ingested_commits}")
        print("Hasta pronto!")
        exit(0)

    elapsed_time = previous_time + (time.time() - start_time)
    delete_time_file()
    hours, remainder = divmod(int(elapsed_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Ampliación de ingesta completada en {hours} horas, {minutes} minutos y {seconds} segundos")

def show_menu():
    while True:
        print("\n=== Menú de Ingesta de Commits ===")
        print("1. Es la primera vez que ejecuto este programa o deseo continuar una ejecución (INICIAL) anterior")
        print("2. Actualizar con nuevos commits recientes")
        print("3. Ampliar ingesta con commits mas antiguos")
        print("4. Salir")
        choice = input("Selecciona una opción (1-4): ")

        start_time = time.time()

        if choice == "1":
            ingest_first_time(start_time)
        elif choice == "2":
            ingest_new_commits(start_time)
        elif choice == "3":
            ingest_older_commits(start_time)
        elif choice == "4":
            print("Saliendo del programa. ¡Hasta pronto!")
            break
        else:
            print("Opción inválida. Por favor, selecciona 1, 2, 3 o 4.")

if __name__ == "__main__":
    show_menu()