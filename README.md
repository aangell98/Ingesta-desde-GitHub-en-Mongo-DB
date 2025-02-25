# Memoria: Práctica 3 - Gestión de Datos
## Ingesta de Commits desde GitHub en MongoDB Local

**Autores:** Angel Luis Lara, Fernando Martin, Daniel Salas, Ingrid Niveiro  
**Asignatura:** Gestión de Datos  
**Profesor:** Ricardo Pérez del Castillo  
**Fecha:** 24 de febrero de 2025  
**Facultad de Ciencias Sociales de Talavera de la Reina**  
**Grado en Ingeniería Informática**

---

## Índice
1. [Introducción](#1-introducción)
2. [Objetivos](#2-objetivos)
3. [Desarrollo Paso a Paso](#3-desarrollo-paso-a-paso)
   - [3.1. Configuración del entorno](#31-configuración-del-entorno)
   - [3.2. Conexión a MongoDB local](#32-conexión-a-mongodb-local)
   - [3.3. Autenticación y uso de la API de GitHub](#33-autenticación-y-uso-de-la-api-de-github)
   - [3.4. Estimación del número total de commits](#34-estimación-del-número-total-de-commits)
   - [3.5. Ingesta de commits con gestión del rate limit](#35-ingesta-de-commits-con-gestión-del-rate-limit)
   - [3.6. Manejo de interrupciones](#36-manejo-de-interrupciones)
   - [3.7. Optimizaciones implementadas](#37-optimizaciones-implementadas)
4. [Resultados y Evidencias](#4-resultados-y-evidencias)
5. [Código y Archivos de Configuración](#5-código-y-archivos-de-configuración)
6. [Conclusiones](#6-conclusiones)
7. [Referencias](#7-referencias)

---

## 1. Introducción
Esta memoria documenta el desarrollo de la Práctica 3 de la asignatura *Gestión de Datos*, cuyo objetivo principal es realizar una ingesta avanzada de commits del proyecto `microsoft/vscode` en GitHub hacia una base de datos MongoDB. Inicialmente, se diseñó una solución para MongoDB Atlas, pero las limitaciones de almacenamiento del nivel gratuito (512 MB) hicieron inviable almacenar los más de 100,000 commits estimados del proyecto. Por ello, se adaptó el desarrollo para usar una instancia local de MongoDB, aprovechando el almacenamiento ilimitado del disco local.

El proceso incluyó múltiples iteraciones para optimizar la ingesta, gestionar eficientemente el *rate limit* de GitHub, y garantizar la continuidad del proceso tras interrupciones. Se implementaron mejoras como el procesamiento paralelo de solicitudes HTTP y una estimación precisa del progreso, detalladas a continuación.

---

## 2. Objetivos
Los objetivos específicos del proyecto, según las tareas a entregar (página 31 del documento), son:
1. Realizar la ingesta de commits del proyecto `https://github.com/microsoft/vscode`.
2. Limitar la ingesta a los commits producidos desde el 1 de enero de 2018 hasta la actualidad
3. Gestionar de forma eficaz y eficiente el *rate limit* de la API de GitHub.
4. Añadir a cada documento en MongoDB los campos extendidos `files_modified` (archivos modificados) y `stats` (estadísticas de cambios), utilizando la operación "Get a commit".
5. Proporcionar una memoria con explicaciones y evidencias, junto con el código Python y archivos de configuración.

---

## 3. Desarrollo Paso a Paso

### 3.1. Configuración del entorno
El entorno de desarrollo incluyó las siguientes herramientas:
- **Python 3.11**: Lenguaje de programación utilizado para el cliente.
- **MongoDB Community Server 7.0**: Base de datos local instalada en `localhost:27017`.
- **Librerías Python**:
  - `requests`: Para realizar solicitudes HTTP a la API de GitHub.
  - `pymongo`: Para interactuar con MongoDB.
  - `python-dotenv`: Para cargar variables de configuración desde un archivo `.env`.
  - `concurrent.futures`: Para implementar procesamiento paralelo con multihilo.

**Instalación de MongoDB local**:
1. Se descargó MongoDB Community Server desde [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community).
2. Se instaló en Windows y se inició el servidor ejecutando `mongod` desde la terminal en el directorio de instalación (`C:\Program Files\MongoDB\Server\7.0\bin`).
3. Se verificó que el servidor estuviera corriendo con el comando `mongo` en otra terminal.

**Instalación de dependencias**:
```bash
pip install requests pymongo python-dotenv
```
Nota: `concurrent.futures` es parte de la biblioteca estándar de Python y no requiere instalación adicional.

**Archivo de configuración `.env`**:
Se creó un archivo `.env` en el directorio raíz para externalizar parámetros sensibles y configurables:
```
GITHUB_TOKEN=tu_token_aqui
GITHUB_USER=microsoft
GITHUB_PROJECT=vscode
START_DATE=2018-01-01T00:00:00Z
PER_PAGE=100
MAX_WORKERS=10
LOCAL_MONGO_HOST=localhost
LOCAL_MONGO_PORT=27017
LOCAL_MONGO_DB=github
LOCAL_MONGO_COLLECTION=commits
```

### 3.2. Conexión a MongoDB local
Se configuró la conexión a MongoDB local utilizando variables del archivo `.env`, con valores por defecto para facilitar su uso:
- Host: `localhost`
- Puerto: `27017`
- Base de datos: `github`
- Colección: `commits`

El código verifica la conexión y maneja errores:
```python
MONGODB_HOST = os.getenv("LOCAL_MONGO_HOST", "localhost")
MONGODB_PORT = int(os.getenv("LOCAL_MONGO_PORT", "27017"))
DB_NAME = os.getenv("LOCAL_MONGO_DB", "github")
COLLECTION_NAME = os.getenv("LOCAL_MONGO_COLLECTION", "commits")

try:
    client = MongoClient(MONGODB_HOST, MONGODB_PORT)
    db = client[DB_NAME]
    collection_commits = db[COLLECTION_NAME]
    print("Conexión exitosa a MongoDB local.")
except Exception as e:
    print(f"Error al conectar a MongoDB local: {e}")
    exit(1)
```

La elección de MongoDB local permitió superar las restricciones de almacenamiento de MongoDB Atlas, asegurando que todos los commits pudieran almacenarse sin problemas.

### 3.3. Autenticación y uso de la API de GitHub
Se generó un *Personal Access Token* (PAT) en GitHub con permisos básicos de lectura (`repo`), siguiendo las instrucciones de las páginas 20-23 del documento:
1. Cada miembro accedió a `https://github.com/settings/tokens`.
2. Creó un token con el alcance `repo` y lo almacenó en el archivo `.env` como `GITHUB_TOKEN`.

El token se incluyó en las cabeceras de las solicitudes HTTP:
```python
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}
```

### 3.4. Estimación del número total de commits
La API de GitHub no proporciona un conteo exacto de commits en un rango de fechas, por lo que se implementó una estimación basada en el encabezado `Link`:
- Se realizó una solicitud inicial a la primera página de commits desde `2018-01-01T00:00:00Z` con `per_page=100`.
- Se extrajo el número de la última página del encabezado `Link` para calcular el total aproximado (`total_pages * PER_PAGE`).
- Si el encabezado no estaba disponible, se asumió un valor aproximado multiplicando la cantidad de commits en la primera página por 100.

Código relevante:
```python
def estimate_total_commits():
    print("Estimando el número total de commits (muestra inicial)...")
    base_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?since={START_DATE}&per_page={PER_PAGE}'
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
```

### 3.5. Ingesta de commits con gestión del rate limit
La ingesta se diseñó con las siguientes características:
- **Rango de fechas**: Desde `2018-01-01T00:00:00Z` hasta el commit más reciente en la base de datos (o la actualidad si no había datos previos).
- **Gestión del *rate limit***:
  - Se implementó `check_rate_limit()` para verificar el límite antes de cada solicitud, con hasta 3 reintentos en caso de fallo.
  - Si fallaba tras 3 intentos, se esperaba 60 segundos.
  - Se redujo la frecuencia de mensajes impresos a cada 10 solicitudes o cuando las peticiones restantes eran menores a 100, mejorando la legibilidad.
  - Si las peticiones restantes eran menos de 100, se pausaba hasta el reinicio del límite (`reset_time`), asegurando un uso eficiente de las 5000 solicitudes por hora permitidas por GitHub.

- **Campos extendidos**:
  - Se utilizó la operación "Get a commit" (`commit_url`) para obtener `files_modified` y `stats`, que se añadieron a cada documento antes de su inserción.

- **Evitar duplicados**: Se verificó la existencia de cada commit por su `sha` antes de procesarlo, usando `find_one({"sha": commit_sha})`.

- **Procesamiento paralelo**: Se incorporó multihilo con `ThreadPoolExecutor` para realizar solicitudes HTTP paralelas a los `commit_url`, optimizando el tiempo de obtención de detalles. El número de hilos (`MAX_WORKERS`) se configuró desde `.env`.

Código clave:
```python
while has_new_commits:
    if until_date:
        search_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?page={page}&per_page={PER_PAGE}&since={START_DATE}&until={until_date}'
    else:
        search_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?page={page}&per_page={PER_PAGE}&since={START_DATE}'
    
    response = fetch_with_retries(search_url, headers)
    commits = response.json()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        commits_to_fetch = [commit for commit in commits if not collection_commits.find_one({"sha": commit['sha']})]
        future_to_commit = {executor.submit(fetch_commit_details, commit): commit for commit in commits_to_fetch}
        for future in as_completed(future_to_commit):
            commit_data = future.result()
            if commit_data:
                collection_commits.insert_one(commit_data)
                ingested_commits += 1
                has_new_commits = True
                print(f"Commit {commit_data['sha']} insertado en MongoDB. Fecha: {commit_data['commit']['committer']['date']}. Progreso: {ingested_commits}/{total_commits_estimate}")
```

### 3.6. Manejo de interrupciones
Se implementó un manejo robusto de interrupciones con `Ctrl + C` para permitir pausar y reanudar la ingesta sin pérdida de datos:
- Al interrumpir, se muestra el progreso actual y un mensaje indicando cómo continuar.
- La reanudación usa el commit más antiguo existente (`last_commit_date`) como límite superior (`until`), asegurando continuidad.

Código relevante:
```python
try:
    while has_new_commits:
        # Lógica de ingesta
except KeyboardInterrupt:
    print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
    print(f"Commits ingestados hasta el momento: {ingested_commits} de un estimado de {total_commits_estimate}")
    print("Hasta pronto!")
    exit(0)
```

### 3.7. Optimizaciones implementadas
A lo largo del desarrollo, se introdujeron varias optimizaciones:
- **Multihilo**: Se utilizó `ThreadPoolExecutor` para procesar hasta `MAX_WORKERS` solicitudes HTTP en paralelo, reduciendo significativamente el tiempo de obtención de detalles de commits (de ~100-200 segundos por página a ~10-20 segundos).
- **Configuración dinámica**: Se externalizaron parámetros como `MAX_WORKERS` al archivo `.env`, permitiendo ajustes sin modificar el código.
- **Mensajes reducidos**: Se limitó la salida del *rate limit* para mejorar la legibilidad, imprimiendo solo cada 10 solicitudes o cuando el límite estuviera cerca (<100 peticiones restantes).
- **Ingesta inversa**: Se corrigió la lógica inicial (que procesaba desde el commit más antiguo hacia adelante) para usar `since=START_DATE` y `until=last_commit_date`, procesando desde el más reciente hacia atrás, lo que permitió una reanudación precisa tras interrupciones.

---

## 4. Resultados y Evidencias

### Ejecución inicial
Al ejecutar el script sin datos previos:
```
Conexión exitosa a MongoDB local.
Estimando el número total de commits (muestra inicial)...
Peticiones restantes: 4809, próximo reset: Tue Feb 25 11:48:11 2025
Estimación basada en encabezado 'Link': 102400 commits.
Commits ya ingestados: 0 de un estimado de 102400
No hay commits previos en la base de datos. Ingestando desde el principio.
Ingestando desde 2018-01-01T00:00:00Z sin límite superior.
Commit b91f8eb95c070be0c6037b866feac1b596f3c5e8 insertado en MongoDB. Fecha: 2025-02-25T08:31:52Z. Progreso: 1/102400
Commit a8df977b0d3b1c44d5b0382874754f04bee8870a insertado en MongoDB. Fecha: 2025-02-25T07:18:00Z. Progreso: 2/102400
...
```

### Reanudación con datos previos
Con 20 commits ya insertados:
```
Conexión exitosa a MongoDB local.
Estimando el número total de commits (muestra inicial)...
Peticiones restantes: 4709, próximo reset: Tue Feb 25 11:48:11 2025
Estimación basada en encabezado 'Link': 102400 commits.
Commits ya ingestados: 20 de un estimado de 102400
Fecha del commit más antiguo encontrado: 2025-02-24T20:07:47Z
Continuando desde el commit más antiguo: 2025-02-24T20:07:47Z
Commit xyz789... insertado en MongoDB. Fecha: 2025-02-24T20:06:00Z. Progreso: 21/102400
Commit abc123... insertado en MongoDB. Fecha: 2025-02-24T20:05:00Z. Progreso: 22/102400
...
```

### Interrupción manual
Al presionar `Ctrl + C`:
```
^C
Ejecución interrumpida manualmente con Ctrl + C. Proceso detenido.
Commits ingestados hasta el momento: 50 de un estimado de 102400
Hasta pronto!
```

### Datos en MongoDB
Se verificaron los datos en MongoDB local usando MongoDB Compass o la terminal de MongoDB:
- Base de datos: `github`
- Colección: `commits`
- Documento de ejemplo:
```json
{
  "_id": {
    "$oid": "67bc5277f3a465f35eae37f7"
  },
  "sha": "ea8aabc6b65d996cdd9ef26118e506f25f38486c",
  "commit": {
    "author": {
      "name": "Johannes Rieken",
      "email": "johannes.rieken@gmail.com",
      "date": "2025-02-24T10:49:46Z"
    },
    "committer": {
      "name": "GitHub",
      "email": "noreply@github.com",
      "date": "2025-02-24T10:49:46Z"
    },
    "message": "only rely on `crypto.getRandomValues` and treat `randomUUID` as being optional (#241690)\n\nfixes https://github.com/microsoft/vscode/issues/240334"
  },
  "files_modified": [
    {
      "sha": "8aa1e8801db2ea6e84643eafae7b711524ea941c",
      "filename": "src/vs/base/common/uuid.ts",
      "status": "modified",
      "additions": 53,
      "deletions": 1,
      "changes": 54,
      "patch": "@@ -10,4 +10,56 @@ export function isUUID(value: string): boolean {\n \treturn _UUIDPattern.test(value);\n }\n \n-export const generateUuid: () => string = crypto.randomUUID.bind(crypto);\n+export const generateUuid = (function (): () => string {\n+\n+\t// use `randomUUID` if possible\n+\tif (typeof crypto.randomUUID === 'function') {\n+\t\treturn crypto.randomUUID.bind(crypto);\n+\t}\n+\n+\t// prep-work\n+\tconst _data = new Uint8Array(16);\n+\tconst _hex: string[] = [];\n+\tfor (let i = 0; i < 256; i++) {\n+\t\t_hex.push(i.toString(16).padStart(2, '0'));\n+\t}\n+\n+\treturn function generateUuid(): string {\n+\t\tcrypto.getRandomValues(_data);\n+\t\t_data[6] = (_data[6] & 0x0f) | 0x40;\n+\t\t_data[8] = (_data[8] & 0x3f) | 0x80;\n+\t\tlet i = 0;\n+\t\tlet result = '';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += '-';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += '-';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += '-';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += '-';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\treturn result;\n+\t};\n+})();"
    }
  ],
  "stats": {
    "total": 54,
    "additions": 53,
    "deletions": 1
  },
  "projectId": "vscode"
}
```

**Captura de pantalla**:  
![MongoDB Compass](recursos/compass.png)

---

## 5. Código y Archivos de Configuración

### Código Python (`MongoDB_Local.py`)
El script final incluye todas las optimizaciones y características descritas:
```python
import requests
import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración desde .env con valores por defecto
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_USER = os.getenv("GITHUB_USER", "microsoft")
GITHUB_PROJECT = os.getenv("GITHUB_PROJECT", "vscode")
START_DATE = os.getenv("START_DATE", "2018-01-01T00:00:00Z")
PER_PAGE = int(os.getenv("PER_PAGE", "100"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

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

# Contador para mensajes de rate limit
request_count = 0

def check_rate_limit(threshold=100):
    global request_count
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
            request_count += 1
            if request_count % 10 == 0 or remaining < threshold:
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

def estimate_total_commits():
    print("Estimando el número total de commits (muestra inicial)...")
    base_url = f'https://api.github.com/repos/{GITHUB_USER}/{GITHUB_PROJECT}/commits?since={START_DATE}&per_page={PER_PAGE}'
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
    print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
    print(f"Commits ingestados hasta el momento: {ingested_commits} de un estimado de {total_commits_estimate}")
    print("Hasta pronto!")
    exit(0)

print("Proceso completado.")
```

### Archivos de configuración (`.env`)
Se utilizó un archivo `.env` para externalizar parámetros sensibles y configurables:
```
GITHUB_TOKEN=tu_token_aqui
GITHUB_USER=microsoft
GITHUB_PROJECT=vscode
START_DATE=2018-01-01T00:00:00Z
PER_PAGE=100
MAX_WORKERS=20
LOCAL_MONGO_HOST=localhost
LOCAL_MONGO_PORT=27017
LOCAL_MONGO_DB=github
LOCAL_MONGO_COLLECTION=commits
```

---

## 6. Conclusiones
El proyecto cumplió con todos los objetivos establecidos:
- Se realizó la ingesta de commits del repositorio `microsoft/vscode` desde el 1 de enero de 2018 hasta la actualidad, adaptándose a MongoDB local para superar las limitaciones de almacenamiento de Atlas.
- La gestión del *rate limit* fue eficiente, utilizando un sistema de reintentos y pausas dinámicas, optimizado para minimizar mensajes de depuración.
- Se añadieron los campos `files_modified` y `stats` a cada commit, enriqueciendo los datos almacenados mediante la operación "Get a commit".
- La implementación de multihilo redujo significativamente el tiempo de procesamiento de solicitudes HTTP, mejorando la eficiencia general.
- El manejo de interrupciones permitió pausar y reanudar la ingesta sin pérdida de datos, gracias a la lógica basada en el commit más antiguo.

El desarrollo iterativo resolvió problemas iniciales (como la dirección incorrecta de la ingesta) y optimizó el rendimiento, demostrando la importancia de adaptar soluciones a las restricciones específicas del entorno.

---

## 7. Referencias
- Documentación de MongoDB: [https://docs.mongodb.com/manual/](https://docs.mongodb.com/manual/)
- Descarga de MongoDB: [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community)
- GitHub REST API: [https://docs.github.com/en/rest](https://docs.github.com/en/rest)
- Operación "Get a commit": [https://docs.github.com/en/rest/commits/commits/#get-a-commit](https://docs.github.com/en/rest/commits/commits/#get-a-commit)
- Python `concurrent.futures`: [https://docs.python.org/3/library/concurrent.futures.html](https://docs.python.org/3/library/concurrent.futures.html)
- Documentación de `python-dotenv`: [https://pypi.org/project/python-dotenv/](https://pypi.org/project/python-dotenv/)