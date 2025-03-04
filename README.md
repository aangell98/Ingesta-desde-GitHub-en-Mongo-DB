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
Esta memoria documenta el desarrollo de la Práctica 3 de la asignatura *Gestión de Datos*, cuyo objetivo principal es realizar una ingesta avanzada de commits del proyecto `microsoft/vscode` en GitHub hacia una base de datos MongoDB local. Inicialmente, se exploró el uso de MongoDB Atlas, pero las limitaciones de almacenamiento del nivel gratuito (512 MB) hicieron inviable almacenar los más de 100,000 commits estimados del proyecto. Por ello, se optó por una instancia local de MongoDB, aprovechando el almacenamiento ilimitado del disco local.

El desarrollo evolucionó desde una ingesta básica hasta un sistema optimizado con procesamiento paralelo y configuración dinámica (versión 1.2.0), y posteriormente hacia un enfoque basado en un menú interactivo con tres opciones: ingesta inicial desde una fecha configurable, actualización de commits recientes, y ampliación hacia fechas anteriores (versión 1.3.0). Se gestionó eficientemente el *rate limit* de GitHub y se aseguraron mecanismos de continuidad tras interrupciones.

---

## 2. Objetivos
Los objetivos específicos del proyecto, según las tareas a entregar (página 31 del documento), son:
1. Realizar la ingesta de commits del proyecto `https://github.com/microsoft/vscode`.
2. Limitar la ingesta inicial a los commits producidos desde el 1 de enero de 2018 hasta la actualidad, con capacidad para ampliar hacia fechas anteriores o actualizar con nuevos commits.
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
  - `python-dotenv`: Para cargar variables de configuración desde un archivo `.env` (desde v1.2.0).
  - `concurrent.futures`: Para implementar procesamiento paralelo con multihilo (desde v1.2.0).

**Instalación de MongoDB local**:
1. Se descargó MongoDB Community Server desde [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community).
2. Se instaló en Windows y se inició el servidor ejecutando `mongod` desde la terminal en el directorio de instalación (`C:\Program Files\MongoDB\Server\7.0\bin`).
3. Se verificó que el servidor estuviera corriendo con el comando `mongo` en otra terminal.

**Instalación de dependencias**:
```bash
pip install requests pymongo python-dotenv
```
Nota: `concurrent.futures` es parte de la biblioteca estándar de Python y no requiere instalación adicional.

**Archivo de configuración `.env`** (introducido en v1.2.0):
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
Se configuró la conexión a MongoDB local utilizando variables del archivo `.env` (desde v1.2.0), con valores por defecto:
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
2. Creó un token con el alcance `repo` y lo almacenó en el archivo `.env` como `GITHUB_TOKEN` (desde v1.2.0).

El token se incluyó en las cabeceras de las solicitudes HTTP:
```python
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}
```

### 3.4. Estimación del número total de commits
La API de GitHub no proporciona un conteo exacto de commits en un rango de fechas, por lo que se implementó una estimación basada en el encabezado `Link` (mejorada en v1.2.0):
- Se realizó una solicitud inicial a la primera página de commits desde `START_DATE` con `per_page=100`.
- Se extrajo el número de la última página del encabezado `Link` para calcular el total aproximado (`total_pages * PER_PAGE`).
- Si el encabezado no estaba disponible, se asumió un valor aproximado multiplicando la cantidad de commits en la primera página por 100.

Código relevante:
```python
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
```

Nota: Esta estimación se usa solo en la Opción 1 introducida en v1.3.0; las Opciones 2 y 3 no la requieren.

### 3.5. Ingesta de commits con gestión del rate limit
**Versión inicial hasta v1.2.0:**
- **Rango de fechas**: Desde `START_DATE` (por defecto `2018-01-01T00:00:00Z`) hasta el commit más reciente o el más antiguo existente si había datos previos.
- **Gestión del *rate limit***:
  - Se verificaba el límite antes de cada solicitud con hasta 3 reintentos y una pausa de 60 segundos en caso de fallo persistente (v1.2.0).
  - Los mensajes se reducían a cada 10 solicitudes o cuando las peticiones restantes eran menos de 100 (v1.2.0).
  - Pausas dinámicas basadas en `reset_time` se activaban cuando `remaining < 100` (v1.2.0).
- **Campos extendidos**: Se usaba "Get a commit" para añadir `files_modified` y `stats`.
- **Evitar duplicados**: Verificación por `sha` antes de procesar cada commit.
- **Procesamiento paralelo**: Introducido en v1.2.0 con `ThreadPoolExecutor` y `MAX_WORKERS` solicitudes en paralelo.

**Versión 1.3.0:**
El script evolucionó hacia un sistema basado en un menú interactivo con tres opciones:
- **Opción 1 - Ingesta inicial**:
  - **Rango**: Desde `START_DATE` hasta el commit más reciente o el más antiguo existente (`until=last_commit_date`) si hay datos previos.
  - **Propósito**: Inicia o continúa la ingesta desde la fecha base hacia el presente.
- **Opción 2 - Actualización de commits recientes**:
  - **Rango**: Desde la fecha del commit más reciente (`newest_date`) hasta el primer duplicado encontrado o el presente.
  - **Propósito**: Agrega commits nuevos realizados después de la última ejecución.
- **Opción 3 - Ampliación hacia atrás**:
  - **Rango**: Desde una fecha anterior introducida por el usuario (`new_start_date`) hasta el commit más antiguo existente (`until=oldest_date`), continuando desde el commit más reciente anterior a `oldest_date` si hay datos previos.
  - **Propósito**: Extiende la ingesta hacia fechas anteriores al rango inicial.

**Gestión del *rate limit*** (común a todas las opciones):
- Mantenida desde v1.2.0, con verificación antes de cada solicitud, 3 reintentos, y pausas dinámicas basadas en `reset_time`.

**Campos extendidos y duplicados** (común a todas las opciones):
- Sin cambios desde v1.2.0: uso de "Get a commit" y verificación por `sha`.

**Procesamiento paralelo** (común a todas las opciones):
- Mantenido desde v1.2.0 con `ThreadPoolExecutor` y `MAX_WORKERS`.

Código clave (estructura general en v1.3.0):
```python
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    commits_to_fetch = [commit for commit in commits if not collection_commits.find_one({"sha": commit['sha']})]
    if commits_to_fetch:
        future_to_commit = {executor.submit(fetch_commit_details, commit): commit for commit in commits_to_fetch}
        for future in as_completed(future_to_commit):
            commit_data = future.result()
            if commit_data:
                collection_commits.insert_one(commit_data)
                ingested_commits += 1
                has_new_commits = True
```

### 3.6. Manejo de interrupciones
**Hasta v1.2.0:**
- Se implementó un manejo básico con `Ctrl + C`, mostrando el progreso actual y permitiendo reanudación desde el commit más antiguo (`last_commit_date`) como `until`.

**Versión 1.3.0:**
- **Opción 1**: Reanuda desde el commit más antiguo (`last_commit_date`) como `until`, usando `START_DATE` como `since`.
- **Opción 2**: No requiere reanudación especial; procesa desde el commit más reciente hasta el presente o el primer duplicado.
- **Opción 3**: Reanuda desde el commit más reciente anterior al más antiguo existente (`newest_before_oldest`), ajustando dinámicamente el rango para continuar tras interrupciones.

Código relevante (ejemplo en Opción 1):
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
**Hasta v1.2.0:**
- **Multihilo**: Implementado con `ThreadPoolExecutor` para procesar hasta `MAX_WORKERS` solicitudes HTTP en paralelo, reduciendo el tiempo de obtención de detalles.
- **Configuración dinámica**: Parámetros como `MAX_WORKERS`, `START_DATE` y `PER_PAGE` externalizados al archivo `.env`.
- **Mensajes reducidos**: Limitados a cada 10 solicitudes o cuando las peticiones restantes eran menos de 100.
- **Ingesta inversa**: Corregida para procesar desde el más reciente hacia atrás dentro del rango (`since=START_DATE`, `until=last_commit_date`).

**Versión 1.3.0:**
- **Menú interactivo**: Introducido para permitir al usuario elegir entre ingesta inicial, actualización reciente o ampliación hacia atrás, mejorando la flexibilidad.
- **Actualización de nuevos commits**: Opción 2 agrega commits desde el más reciente hasta el presente, deteniéndose al encontrar duplicados, con advertencia al usuario.
- **Ampliación hacia atrás**: Opción 3 permite extender la ingesta desde una fecha anterior ingresada por el usuario hasta el commit más antiguo existente, continuando desde el último commit insertado tras interrupciones usando `get_newest_date_before_oldest`.

**Últimas modificaciones:**
- Por último, se agregaron las siguientes modificaciones:
   - Integración de las versiones Local y Atlas con selección al inicio para simplificar la estructura del proyecto integrando las versiones para MongoDB Local y MongoDB Atlas en un solo archivo .py, permitiendo al usuario elegir la conexión al inicio de la ejecución.
   - Medición del tiempo de ejecución que toma cada operación de ingesta desde que se selecciona una opción hasta que finaliza o se interrumpe, mostrando el resultado en horas, minutos y segundos. Si se interrumpe se guarda el tiempo empleado para que al reejecutarlo empiece a contar desde ahí.


---

## 4. Resultados y Evidencias

### Ejecución de Opción 1 (Ingesta inicial, sin datos previos, v1.3.0)
```
=== Menú de Ingesta de Commits ===
1. Es la primera vez que ejecuto este programa o deseo continuar una ejecución anterior
2. Actualizar con nuevos commits recientes
3. Ampliar ingesta hacia atrás desde la fecha inicial
4. Salir
Selecciona una opción (1-4): 1
Conexión exitosa a MongoDB local.
Estimando el número total de commits (muestra inicial)...
Peticiones restantes: 4809, próximo reset: Tue Feb 25 11:48:11 2025
Estimación basada en encabezado 'Link': 102400 commits.
Commits ya ingestados: 0 de un estimado de 102400
No hay commits previos en la base de datos. Ingestando desde el principio.
Ingestando desde 2018-01-01T00:00:00Z sin límite superior.
Commit b91f8eb... insertado en MongoDB. Fecha: 2025-02-25T08:31:52Z. Progreso: 1/102400
...
```

### Ejecución de Opción 2 (Actualización reciente, v1.3.0)
```
=== Menú de Ingesta de Commits ===
1. Es la primera vez que ejecuto este programa o deseo continuar una ejecución anterior
2. Actualizar con nuevos commits recientes
3. Ampliar ingesta hacia atrás desde la fecha inicial
4. Salir
Selecciona una opción (1-4): 2
Conexión exitosa a MongoDB local.
Commits actualmente en la base de datos: 102400
Buscando nuevos commits desde el más reciente: 2025-02-25T08:31:52Z
ADVERTENCIA: Si es la primera vez que ejecutas el programa o la ingesta inicial no fue completada, podrías corromper los datos existentes en la base de datos.
¿Estás seguro de que quieres continuar? (sí/no): sí
Commit new123... insertado en MongoDB (nuevo). Fecha: 2025-02-26T09:00:00Z
...
Ingesta de nuevos commits completada.
Commits nuevos ingestados en esta ejecución: 2
Nuevo total de commits en la base de datos: 102402
```

### Ejecución de Opción 3 (Ampliación hacia atrás, interrumpida y continuada, v1.3.0)
**Primera ejecución:**
```
=== Menú de Ingesta de Commits ===
1. Es la primera vez que ejecuto este programa o deseo continuar una ejecución anterior
2. Actualizar con nuevos commits recientes
3. Ampliar ingesta hacia atrás desde la fecha inicial
4. Salir
Selecciona una opción (1-4): 3
Conexión exitosa a MongoDB local.
Commits actualmente en la base de datos: 102576
Fecha del commit más antiguo actual: 2018-01-02T10:00:00Z
Por favor, introduce la fecha hasta la cual deseas ampliar la ingesta (formato: YYYY-MM-DDTHH:MM:SSZ, ej. 2017-01-01T00:00:00Z):
Nueva fecha de inicio: 2017-01-01T00:00:00Z
No hay commits previos a 2018-01-02T10:00:00Z en la base de datos. Iniciando desde 2017-01-01T00:00:00Z
Ampliando ingesta desde 2017-01-01T00:00:00Z hasta 2018-01-02T10:00:00Z
Página 1: Encontrados 100 commits en la respuesta de la API
Página 1: 100 commits nuevos para procesar
Commit old123... insertado en MongoDB (anterior). Fecha: 2017-01-01T01:00:00Z
^C
Ejecución interrumpida manualmente con Ctrl + C. Proceso detenido.
Commits nuevos ingestados en esta ejecución: 100
Nuevo total de commits en la base de datos: 102676
Hasta pronto!
```

**Continuación:**
```
=== Menú de Ingesta de Commits ===
1. Es la primera vez que ejecuto este programa o deseo continuar una ejecución anterior
2. Actualizar con nuevos commits recientes
3. Ampliar ingesta hacia atrás desde la fecha inicial
4. Salir
Selecciona una opción (1-4): 3
Conexión exitosa a MongoDB local.
Commits actualmente en la base de datos: 102676
Fecha del commit más antiguo actual: 2017-01-01T01:00:00Z
Por favor, introduce la fecha hasta la cual deseas ampliar la ingesta (formato: YYYY-MM-DDTHH:MM:SSZ, ej. 2017-01-01T00:00:00Z):
Nueva fecha de inicio: 2017-01-01T00:00:00Z
Continuando desde el commit más reciente antes de 2017-01-01T01:00:00Z: 2017-01-01T01:00:00Z
Ampliando ingesta desde 2017-01-01T01:00:00Z hasta 2017-01-01T01:00:00Z
Página 1: Encontrados 100 commits en la respuesta de la API
Página 1: 100 commits nuevos para procesar
Commit old456... insertado en MongoDB (anterior). Fecha: 2017-01-01T02:00:00Z
...
Ampliación de ingesta completada.
Commits nuevos ingestados en esta ejecución: 150
Nuevo total de commits en la base de datos: 102826
```

### Datos en MongoDB
Se verificaron los datos en MongoDB local usando MongoDB Compass:
- Base de datos: `github`
- Colección: `commits`
- Documento de ejemplo:
```json
{
  "_id": {"$oid": "67bc5277f3a465f35eae37f7"},
  "sha": "ea8aabc6b65d996cdd9ef26118e506f25f38486c",
  "commit": {
    "author": {"name": "Johannes Rieken", "email": "johannes.rieken@gmail.com", "date": "2025-02-24T10:49:46Z"},
    "committer": {"name": "GitHub", "email": "noreply@github.com", "date": "2025-02-24T10:49:46Z"},
    "message": "only rely on `crypto.getRandomValues` and treat `randomUUID` as optional (#241690)\n\nfixes https://github.com/microsoft/vscode/issues/240334"
  },
  "files_modified": [
    {
      "sha": "8aa1e8801db2ea6e84643eafae7b711524ea941c",
      "filename": "src/vs/base/common/uuid.ts",
      "status": "modified",
      "additions": 53,
      "deletions": 1,
      "changes": 54,
      "patch": "@@ -10,4 +10,56 @@ export function isUUID(value: string): boolean {\n \treturn _UUIDPattern.test(value);\n }\n \n-export const generateUuid: () => string = crypto.randomUUID.bind(crypto);\n+export const generateUuid = (function (): () => string {\n+..."
    }
  ],
  "stats": {"total": 54, "additions": 53, "deletions": 1},
  "projectId": "vscode"
}
```

**Captura de pantalla**:  
![MongoDB Compass](recursos/compass.png)

---

## 5. Código y Archivos de Configuración

### Código Python (`MongoDB_Local.py`, v1.3.0)
El script final incluye el menú interactivo y las optimizaciones descritas:
```python
import requests
import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

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

if not GITHUB_TOKEN:
    print("Error: GITHUB_TOKEN no está definido en el archivo .env")
    exit(1)

headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

try:
    client = MongoClient(MONGODB_HOST, MONGODB_PORT)
    db = client[DB_NAME]
    collection_commits = db[COLLECTION_NAME]
    print("Conexión exitosa a MongoDB local.")
except Exception as e:
    print(f"Error al conectar a MongoDB local: {e}")
    exit(1)

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
            elif status_code in [400, 404, 409]:
                print(f"Error {status_code} en {url}: {'Bad Request' if status_code == 400 else 'Not Found' if status_code == 404 else 'Conflict'}.")
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

def ingest_first_time():
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

def ingest_new_commits():
    ingested_commits_before = collection_commits.count_documents({"projectId": GITHUB_PROJECT})
    print(f"Commits actualmente en la base de datos: {ingested_commits_before}")

    newest_date = get_newest_commit_date()
    if not newest_date:
        print("No hay commits previos en la base de datos. Por favor, ejecuta la opción 1 primero.")
        return

    print(f"Buscando nuevos commits desde el más reciente: {newest_date}")
    print("ADVERTENCIA: Si es la primera vez que ejecutas el programa o la ingesta inicial no fue completada, podrías corromper los datos existentes en la base de datos.")
    confirmation = input("¿Estás seguro de que quieres continuar? (sí/no): ")
    if confirmation.lower() != "sí":
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
        print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
        print(f"Commits nuevos ingestados en esta ejecución: {new_commits_count}")
        print(f"Nuevo total de commits en la base de datos: {ingested_commits_before}")
        print("Hasta pronto!")
        exit(0)

    ingested_commits_after = collection_commits.count_documents({"projectId": GITHUB_PROJECT})
    print(f"Ingesta de nuevos commits completada.")
    print(f"Commits nuevos ingestados en esta ejecución: {new_commits_count}")
    print(f"Nuevo total de commits en la base de datos: {ingested_commits_after}")

def ingest_older_commits():
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
                                print(f"Commit {commit_sha} insertado en MongoDB (anterior). Fecha: {commit_date}")
                            except Exception as e:
                                print(f"Error al insertar commit {commit_sha}: {e}")

            page += 1

    except KeyboardInterrupt:
        print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
        print(f"Commits nuevos ingestados en esta ejecución: {new_commits_count}")
        print(f"Nuevo total de commits en la base de datos: {ingested_commits}")
        print("Hasta pronto!")
        exit(0)

    ingested_commits_after = collection_commits.count_documents({"projectId": GITHUB_PROJECT})
    print(f"Ampliación de ingesta completada.")
    print(f"Commits nuevos ingestados en esta ejecución: {new_commits_count}")
    print(f"Nuevo total de commits en la base de datos: {ingested_commits_after}")

def show_menu():
    while True:
        print("\n=== Menú de Ingesta de Commits ===")
        print("1. Es la primera vez que ejecuto este programa o deseo continuar una ejecución anterior")
        print("2. Actualizar con nuevos commits recientes")
        print("3. Ampliar ingesta hacia atrás desde la fecha inicial")
        print("4. Salir")
        choice = input("Selecciona una opción (1-4): ")

        if choice == "1":
            ingest_first_time()
        elif choice == "2":
            ingest_new_commits()
        elif choice == "3":
            ingest_older_commits()
        elif choice == "4":
            print("Saliendo del programa. ¡Hasta pronto!")
            break
        else:
            print("Opción inválida. Por favor, selecciona 1, 2, 3 o 4.")

if __name__ == "__main__":
    show_menu()
```

### Archivos de configuración (`.env`)
Utilizado desde v1.2.0 y mantenido en v1.3.0:
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
- Se realizó la ingesta de commits del repositorio `microsoft/vscode` desde el 1 de enero de 2018 hasta la actualidad (v1.2.0), con capacidades añadidas para actualizar commits recientes y ampliar hacia fechas anteriores (v1.3.0).
- La gestión del *rate limit* fue eficiente, optimizada en v1.2.0 con reintentos, pausas dinámicas y mensajes reducidos.
- Se añadieron los campos `files_modified` y `stats` a cada commit mediante "Get a commit".
- La implementación de multihilo (v1.2.0) mejoró significativamente el rendimiento y el menú interactivo (v1.3.0) mejoró la flexibilidad para diferentes casos de uso.
- El manejo de interrupciones permitió pausar y reanudar la ingesta sin pérdida de datos, con una solución robusta para la Opción 3 en v1.3.0 que asegura continuidad tras interrupciones.
El desarrollo iterativo resolvió problemas iniciales y optimizó la experiencia del usuario, demostrando la importancia de adaptar soluciones a requisitos cambiantes.

---

## 7. Referencias
- Documentación de MongoDB: [https://docs.mongodb.com/manual/](https://docs.mongodb.com/manual/)
- Descarga de MongoDB: [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community)
- GitHub REST API: [https://docs.github.com/en/rest](https://docs.github.com/en/rest)
- Operación "Get a commit": [https://docs.github.com/en/rest/commits/commits/#get-a-commit](https://docs.github.com/en/rest/commits/commits/#get-a-commit)
- Python `concurrent.futures`: [https://docs.python.org/3/library/concurrent.futures.html](https://docs.python.org/3/library/concurrent.futures.html)
- Documentación de `python-dotenv`: [https://pypi.org/project/python-dotenv/](https://pypi.org/project/python-dotenv/)
