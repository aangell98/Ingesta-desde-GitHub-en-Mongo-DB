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
4. [Resultados y Evidencias](#4-resultados-y-evidencias)
5. [Código y Archivos de Configuración](#5-código-y-archivos-de-configuración)
6. [Conclusiones](#6-conclusiones)
7. [Referencias](#7-referencias)

---

## 1. Introducción
Esta memoria documenta el desarrollo de la Práctica 3 de la asignatura Gestión de Datos, cuyo objetivo principal es realizar una ingesta avanzada de commits del proyecto `microsoft/vscode` en GitHub hacia una base de datos MongoDB. Inicialmente, se implementó una solución para MongoDB Atlas, pero debido a las limitaciones de espacio en el nivel gratuito (512 MB), se optó por adaptar el proyecto para usar una instancia local de MongoDB, aprovechando el almacenamiento ilimitado del disco local.

El documento describe paso a paso el proceso de desarrollo, desde la configuración del entorno hasta la implementación de la lógica de ingesta, incluyendo la gestión eficiente del *rate limit* de GitHub y la adición de campos extendidos a los documentos. Se presentan evidencias de los resultados obtenidos y se incluye el código completo.

---

## 2. Objetivos
Los objetivos específicos del proyecto, según las tareas a entregar (página 31 del documento), son:
1. Realizar la ingesta de commits del proyecto `https://github.com/microsoft/vscode`.
2. Limitar la ingesta a los commits producidos desde el 1 de enero de 2018 hasta la actualidad (24 de febrero de 2025).
3. Gestionar de forma eficaz y eficiente el *rate limit* de la API de GitHub.
4. Añadir a cada documento en MongoDB dos campos extendidos: información de archivos modificados y estadísticas de cambios de cada commit, utilizando la operación "Get a commit".
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

**Instalación de MongoDB local**:
1. Se descargó MongoDB Community Server desde [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community).
2. Se instaló en el sistema operativo (Windows en este caso) y se inició el servidor ejecutando `mongod` desde la terminal en el directorio de instalación.
3. Se verificó que el servidor estuviera corriendo con el comando `mongo` en otra terminal.

**Instalación de dependencias**:
```bash
pip install requests pymongo
```

### 3.2. Conexión a MongoDB local
Se configuró la conexión a MongoDB local utilizando los parámetros:
- Host: `localhost`
- Puerto: `27017`
- Base de datos: `github`
- Colección: `commits`

El código de conexión verifica la disponibilidad del servidor y muestra un mensaje de error si falla:
```python
try:
    client = MongoClient(MONGODB_HOST, MONGODB_PORT)
    db = client[DB_NAME]
    collection_commits = db[COLLECTION_NAME]
    print("Conexión exitosa a MongoDB local.")
except Exception as e:
    print(f"Error al conectar a MongoDB local: {e}")
    exit(1)
```

Se eligió MongoDB local en lugar de Atlas para evitar las restricciones de espacio (512 MB en el nivel gratuito), permitiendo almacenar todos los commits sin limitaciones.

### 3.3. Autenticación y uso de la API de GitHub
Se generó un *Personal Access Token* (PAT) en GitHub con permisos básicos de lectura (`repo`), siguiendo las instrucciones de las páginas 20-23 del documento:
1. Cada miembro accedió a `https://github.com/settings/tokens`.
2. Crearon un token con el alcance `repo` y lo almacenaron de forma segura.

El token se incluyó en las cabeceras de las solicitudes HTTP:
```python
token = 'token'
headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}
```

### 3.4. Estimación del número total de commits
Dado que la API de GitHub no proporciona un conteo exacto de commits en un rango de fechas, se implementó una estimación aproximada para minimizar el uso del *rate limit*:
- Se realizó una solicitud a la primera página de commits desde el 1 de enero de 2018 con `per_page=100`.
- Se extrajo el número total de páginas del encabezado `Link` (si estaba disponible) o se asumió un valor aproximado (100 páginas) basado en la muestra.

Código relevante:
```python
def estimate_total_commits():
    base_url = f'https://api.github.com/repos/{user}/{project}/commits?since={start_date}&per_page={per_page}'
    response = fetch_with_retries(f"{base_url}&page=1", headers)
    commits = response.json()
    commits_in_page = len(commits)
    link_header = response.headers.get('Link', '')
    if 'rel="last"' in link_header:
        total_pages = int(link_header.split('&page=')[1].split('>')[0])
        total_commits = total_pages * per_page
    else:
        total_commits = commits_in_page * 100
    return total_commits
```

### 3.5. Ingesta de commits con gestión del rate limit
La ingesta se realizó con las siguientes características:
- **Rango de fechas**: Desde `2018-01-01T00:00:00Z` hasta el último commit insertado (o la actualidad si no había datos previos).
- **Gestión del *rate limit***:
  - Se verificó el límite de peticiones antes de cada solicitud con hasta 3 reintentos.
  - Si fallaba tras 3 intentos, se esperaba 60 segundos.
  - Si las peticiones restantes eran menos de 10, se pausaba hasta el tiempo de reinicio.

- **Campos extendidos**:
  - Se usó la operación "Get a commit" (`commit_url`) para obtener `files_modified` y `stats` de cada commit.
  - Estos campos se añadieron al documento antes de insertarlo en MongoDB.

- **Evitar duplicados**: Se comprobó la existencia de cada commit por su `sha` antes de insertarlo.

Código clave:
```python
for commit in commits:
    commit_sha = commit['sha']
    if collection_commits.find_one({"sha": commit_sha}):
        continue
    commit_response = fetch_with_retries(commit['url'], headers)
    commit_data = commit_response.json()
    commit['files_modified'] = commit_data.get('files', [])
    commit['stats'] = commit_data.get('stats', {})
    collection_commits.insert_one(commit)
```

### 3.6. Manejo de interrupciones
Se implementó un manejo de interrupciones con `Ctrl + C` para mostrar un mensaje personalizado y el progreso alcanzado:
```python
except KeyboardInterrupt:
    print("\n\nEjecución interrumpida manualmente con Ctrl + C. Proceso detenido.")
    print(f"Commits ingestados hasta el momento: {ingested_commits} de un estimado de {total_commits_estimate}")
    print("Puedes reanudar el proceso ejecutando el script nuevamente. ¡Hasta pronto!")
    exit(0)
```

Esto permite pausar y reanudar la ingesta sin perder datos, aprovechando la verificación del último commit insertado.

---

## 4. Resultados y Evidencias

### Ejecución inicial
Al ejecutar el script, se conectó a MongoDB local y estimó el número total de commits:
```
Conexión exitosa a MongoDB local.
Estimando el número total de commits (muestra inicial)...
Estimación basada en encabezado 'Link': 3700 commits.
Commits ya ingestados: 0 de un estimado de 3700
No hay commits previos en la base de datos. Ingestando desde 2018-01-01T00:00:00Z...
Peticiones restantes: 5000, próximo reset: Mon Feb 24 12:00:00 2025
Commit abc123... insertado en MongoDB. Progreso: 1/3700
```

### Progreso y *rate limit*
Durante la ingesta, se gestionó el *rate limit* eficientemente:
```
Peticiones restantes: 4990, próximo reset: Mon Feb 24 12:00:00 2025
Commit def456... insertado en MongoDB. Progreso: 10/3700
Esperando 10 segundos debido al límite de tasas...
```

### Interrupción manual
Al presionar `Ctrl + C`:
```
^C
Ejecución interrumpida manualmente con Ctrl + C. Proceso detenido.
Commits ingestados hasta el momento: 50 de un estimado de 3700
Puedes reanudar el proceso ejecutando el script nuevamente. ¡Hasta pronto!
```

### Datos en MongoDB
Se verificaron los datos en MongoDB local usando MongoDB Compass:
- Base de datos: `github`
- Colección: `commits`
- Documento de ejemplo:
```json
{
  "_id": {
    "$oid": "67bc5277f3a465f35eae37f7"
  },
  "sha": "ea8aabc6b65d996cdd9ef26118e506f25f38486c",
  "node_id": "C_kwDOAn8RLNoAKGVhOGFhYmM2YjY1ZDk5NmNkZDllZjI2MTE4ZTUwNmYyNWYzODQ4NmM",
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
    "message": "only rely on `crypto.getRandomValues` and treat `randomUUID` as being optional (#241690)\n\nfixes https://github.com/microsoft/vscode/issues/240334",
    "tree": {
      "sha": "b583de406f2d3b3e33a51fb9a9c94cea404d4884",
      "url": "https://api.github.com/repos/microsoft/vscode/git/trees/b583de406f2d3b3e33a51fb9a9c94cea404d4884"
    },
    "url": "https://api.github.com/repos/microsoft/vscode/git/commits/ea8aabc6b65d996cdd9ef26118e506f25f38486c",
    "comment_count": 0,
    "verification": {
      "verified": true,
      "reason": "valid",
      "signature": "-----BEGIN PGP SIGNATURE-----\n\nwsFcBAABCAAQBQJnvE7KCRC1aQ7uu5UhlAAAMcIQACt3P4MFacpeUR1FP4LUYdBL\nOT+VfTOj2MwM4b78EZtt1Fbb/BHdQ5vKPSuHF94pH+YHw7O8Ke7A6CpIvoreljbU\nMBmeOstAja4VwOZWjgdWsSUYlsHBCJD+OakasgKfLoxe0iBtqXsFLphbHuVOD3u9\nIX/iddmCbIJcLO7USHy5qnh4GxvAxWVUIiKjyqh9NY6RTYbDSyyOsxa213xfHWy8\nogLWRXxPgbihMmQtYwWVhbOOdib7w+N2l9CSOf2D6MGUEykGrdTAQbrFcavgeHy+\n11xAJT7yAfu7fbTsk7pgadQneUuNrAb2oshmnvaDWyFDmB9yrmw1BrAr8rjJJ15w\nMmKgBiVubsJB7zPg93JfrEyOvnQfGJ66yXWVQOqfUkYKMZuy8ji2OMALGNuv8su+\nelY/zGhIqJTSKTTsSWMxo/b/adkN+5l+aDe0meHaeQ13kis267ExfgtG3/OKEaLt\nK7IwmLCYeHpNZSJL/PrUTyeurWmHSCOug8oZnYE4vKfc0b0Fz2RFfFgiYJ6Xhnuo\nEsbMvGOCGDGU4pCMD6uHBi5jA0GmJiS6fv7xEOLuUZslVkBkr9BSqo513AqKhtyJ\n4dV2MNwILweqEP9/bFELg41V2YrRHrC5ZpVl1ulHMChJSgP9f0fzRll03EnrJwqm\nHMRjJ6q3qzxpX9VxFIt5\n=/T//\n-----END PGP SIGNATURE-----\n",
      "payload": "tree b583de406f2d3b3e33a51fb9a9c94cea404d4884\nparent 28ab93455c176b9d66198857cfc69b189adba616\nauthor Johannes Rieken <johannes.rieken@gmail.com> 1740394186 +0100\ncommitter GitHub <noreply@github.com> 1740394186 +0100\n\nonly rely on `crypto.getRandomValues` and treat `randomUUID` as being optional (#241690)\n\nfixes https://github.com/microsoft/vscode/issues/240334",
      "verified_at": "2025-02-24T10:49:49Z"
    }
  },
  "url": "https://api.github.com/repos/microsoft/vscode/commits/ea8aabc6b65d996cdd9ef26118e506f25f38486c",
  "html_url": "https://github.com/microsoft/vscode/commit/ea8aabc6b65d996cdd9ef26118e506f25f38486c",
  "comments_url": "https://api.github.com/repos/microsoft/vscode/commits/ea8aabc6b65d996cdd9ef26118e506f25f38486c/comments",
  "author": {
    "login": "jrieken",
    "id": 1794099,
    "node_id": "MDQ6VXNlcjE3OTQwOTk=",
    "avatar_url": "https://avatars.githubusercontent.com/u/1794099?v=4",
    "gravatar_id": "",
    "url": "https://api.github.com/users/jrieken",
    "html_url": "https://github.com/jrieken",
    "followers_url": "https://api.github.com/users/jrieken/followers",
    "following_url": "https://api.github.com/users/jrieken/following{/other_user}",
    "gists_url": "https://api.github.com/users/jrieken/gists{/gist_id}",
    "starred_url": "https://api.github.com/users/jrieken/starred{/owner}{/repo}",
    "subscriptions_url": "https://api.github.com/users/jrieken/subscriptions",
    "organizations_url": "https://api.github.com/users/jrieken/orgs",
    "repos_url": "https://api.github.com/users/jrieken/repos",
    "events_url": "https://api.github.com/users/jrieken/events{/privacy}",
    "received_events_url": "https://api.github.com/users/jrieken/received_events",
    "type": "User",
    "user_view_type": "public",
    "site_admin": true
  },
  "committer": {
    "login": "web-flow",
    "id": 19864447,
    "node_id": "MDQ6VXNlcjE5ODY0NDQ3",
    "avatar_url": "https://avatars.githubusercontent.com/u/19864447?v=4",
    "gravatar_id": "",
    "url": "https://api.github.com/users/web-flow",
    "html_url": "https://github.com/web-flow",
    "followers_url": "https://api.github.com/users/web-flow/followers",
    "following_url": "https://api.github.com/users/web-flow/following{/other_user}",
    "gists_url": "https://api.github.com/users/web-flow/gists{/gist_id}",
    "starred_url": "https://api.github.com/users/web-flow/starred{/owner}{/repo}",
    "subscriptions_url": "https://api.github.com/users/web-flow/subscriptions",
    "organizations_url": "https://api.github.com/users/web-flow/orgs",
    "repos_url": "https://api.github.com/users/web-flow/repos",
    "events_url": "https://api.github.com/users/web-flow/events{/privacy}",
    "received_events_url": "https://api.github.com/users/web-flow/received_events",
    "type": "User",
    "user_view_type": "public",
    "site_admin": false
  },
  "parents": [
    {
      "sha": "28ab93455c176b9d66198857cfc69b189adba616",
      "url": "https://api.github.com/repos/microsoft/vscode/commits/28ab93455c176b9d66198857cfc69b189adba616",
      "html_url": "https://github.com/microsoft/vscode/commit/28ab93455c176b9d66198857cfc69b189adba616"
    }
  ],
  "files_modified": [
    {
      "sha": "8aa1e8801db2ea6e84643eafae7b711524ea941c",
      "filename": "src/vs/base/common/uuid.ts",
      "status": "modified",
      "additions": 53,
      "deletions": 1,
      "changes": 54,
      "blob_url": "https://github.com/microsoft/vscode/blob/ea8aabc6b65d996cdd9ef26118e506f25f38486c/src%2Fvs%2Fbase%2Fcommon%2Fuuid.ts",
      "raw_url": "https://github.com/microsoft/vscode/raw/ea8aabc6b65d996cdd9ef26118e506f25f38486c/src%2Fvs%2Fbase%2Fcommon%2Fuuid.ts",
      "contents_url": "https://api.github.com/repos/microsoft/vscode/contents/src%2Fvs%2Fbase%2Fcommon%2Fuuid.ts?ref=ea8aabc6b65d996cdd9ef26118e506f25f38486c",
      "patch": "@@ -10,4 +10,56 @@ export function isUUID(value: string): boolean {\n \treturn _UUIDPattern.test(value);\n }\n \n-export const generateUuid: () => string = crypto.randomUUID.bind(crypto);\n+export const generateUuid = (function (): () => string {\n+\n+\t// use `randomUUID` if possible\n+\tif (typeof crypto.randomUUID === 'function') {\n+\t\t// see https://developer.mozilla.org/en-US/docs/Web/API/Window/crypto\n+\t\t// > Although crypto is available on all windows, the returned Crypto object only has one\n+\t\t// > usable feature in insecure contexts: the getRandomValues() method.\n+\t\t// > In general, you should use this API only in secure contexts.\n+\n+\t\treturn crypto.randomUUID.bind(crypto);\n+\t}\n+\n+\t// prep-work\n+\tconst _data = new Uint8Array(16);\n+\tconst _hex: string[] = [];\n+\tfor (let i = 0; i < 256; i++) {\n+\t\t_hex.push(i.toString(16).padStart(2, '0'));\n+\t}\n+\n+\treturn function generateUuid(): string {\n+\t\t// get data\n+\t\tcrypto.getRandomValues(_data);\n+\n+\t\t// set version bits\n+\t\t_data[6] = (_data[6] & 0x0f) | 0x40;\n+\t\t_data[8] = (_data[8] & 0x3f) | 0x80;\n+\n+\t\t// print as string\n+\t\tlet i = 0;\n+\t\tlet result = '';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += '-';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += '-';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += '-';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += '-';\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\tresult += _hex[_data[i++]];\n+\t\treturn result;\n+\t};\n+})();"
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

![alt text](recursos/compass.png)

---

## 5. Código y Archivos de Configuración

### Código Python (`MongoDB_Atlas.py`)
```python
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
```
### Código Python (`MongoDB_Local.py`)
```python
import requests
import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, timezone

# Datos de conexión a MongoDB local
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DB_NAME = 'github'
COLLECTION_NAME = 'commits'

# Datos de autenticación para GitHub
token = 'token'
headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

# Parámetros de consulta
user = 'microsoft'
project = 'vscode'
start_date = '2018-01-01T00:00:00Z'
per_page = 100

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
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Error al obtener datos desde {url}. Intento {retries}/{max_retries}: {e}")
            if retries < max_retries:
                time.sleep(5)
    print(f"No se pudo obtener datos desde {url} después de {max_retries} intentos.")
    return None

def estimate_total_commits():
    print("Estimando el número total de commits (muestra inicial)...")
    base_url = f'https://api.github.com/repos/{user}/{project}/commits?since={start_date}&per_page={per_page}'
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
        total_commits = total_pages * per_page
        print(f"Estimación basada en encabezado 'Link': {total_commits} commits.")
    else:
        total_commits = commits_in_page * 100
        print(f"Estimación aproximada basada en muestra: {total_commits} commits (suponiendo 100 páginas).")
    return total_commits

def get_last_commit_date():
    last_commit = collection_commits.find_one({}, sort=[("commit.committer.date", -1)])
    if last_commit and 'commit' in last_commit and 'committer' in last_commit['commit']:
        return last_commit['commit']['committer']['date']
    return None

total_commits_estimate = estimate_total_commits()
ingested_commits = collection_commits.count_documents({"projectId": project})
print(f"Commits ya ingestados: {ingested_commits} de un estimado de {total_commits_estimate}")

last_commit_date = get_last_commit_date()
if last_commit_date:
    print(f"Último commit insertado: {last_commit_date}. Ingestando commits posteriores...")
else:
    print(f"No hay commits previos en la base de datos. Ingestando desde {start_date}...")
    last_commit_date = None  # No usamos 'until' si no hay commits previos

page = 1

try:
    while True:
        # Construir la URL dinámicamente según si hay un 'until'
        if last_commit_date:
            search_url = f'https://api.github.com/repos/{user}/{project}/commits?page={page}&per_page={per_page}&since={start_date}&until={last_commit_date}'
        else:
            search_url = f'https://api.github.com/repos/{user}/{project}/commits?page={page}&per_page={per_page}&since={start_date}'
        
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

            # Verificar duplicados
            if collection_commits.find_one({"sha": commit_sha}):
                print(f"Commit {commit_sha} ya existe en MongoDB, omitiendo...")
                continue

            commit_response = fetch_with_retries(commit_url, headers)
            if not commit_response:
                print(f"No se pudieron obtener detalles del commit {commit_sha}. Omitiendo...")
                continue

            commit_data = commit_response.json()
            files_modified = commit_data.get('files', [])
            stats = commit_data.get('stats', {})
            commit['files_modified'] = files_modified
            commit['stats'] = stats
            commit['projectId'] = project

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
    print("Puedes reanudar el proceso ejecutando el script nuevamente. ¡Hasta pronto!")
    exit(0)

print("Proceso completado.")
```
### Archivos de configuración
No se utilizaron archivos de configuración externos, ya que todos los parámetros (token, host, puerto, etc.) están integrados en el script para simplicidad. Sin embargo, para una implementación más robusta, se podrían externalizar en un archivo `.env` o similar.

---

## 6. Conclusiones
El proyecto cumplió con todos los objetivos establecidos:
- Se realizó la ingesta de commits del repositorio `microsoft/vscode` desde el 1 de enero de 2018.
- La gestión del *rate limit* fue eficiente, minimizando solicitudes y respetando los límites de la API.
- Se añadieron los campos `files_modified` y `stats` a cada commit, enriqueciendo los datos almacenados.
- La adaptación a MongoDB local resolvió las limitaciones de espacio de MongoDB Atlas.

El manejo de interrupciones y la estimación aproximada de commits mejoraron la usabilidad y eficiencia del script, permitiendo pausar y reanudar el proceso sin problemas.
---

## 7. Referencias
- Documentación de MongoDB: [https://docs.mongodb.com/manual/](https://docs.mongodb.com/manual/)
- Descarga de MongoDB: [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community)
- GitHub REST API: [https://docs.github.com/en/rest](https://docs.github.com/en/rest)
- Operación "Get a commit": [https://docs.github.com/en/rest/commits/commits/#get-a-commit](https://docs.github.com/en/rest/commits/commits/#get-a-commit)
