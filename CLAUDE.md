# nelo-test

Pipeline en dos pasos, sobre PySpark, para consumir eventos de analítica (estilo GA4 e-commerce) desde SQS y dejarlos aplanados en un CSV. Corre de dos formas:

1. **Local** (`make pipeline`): Spark `local[*]` sobre el venv del host, sin Docker. Ver "Comandos" abajo.
2. **Orquestado** (`make stack-up`): los mismos dos scripts, sin modificar, enviados como jobs al cluster de Spark standalone vía un DAG de Airflow. Ver "Stack Airflow + Spark (Docker)" más abajo.

Cada script expone una función `run(...)` de parámetros explícitos, separada del `main()` que solo parsea CLI — por eso el mismo archivo sirve como script local y como aplicación de `spark-submit`.

## Flujo de datos

```
SQS (data-engineering-case-analytics-queue)
  → spark/read_queue.py        → data/raw_messages/   (tabla Parquet, upsert por message_id)
  → spark/transform_messages.py → output/items_flat.csv (upsert por id = sha256(message_id + item_id))
```

`spark/` es la carpeta que se monta tal cual dentro del stack de Docker como `/opt/spark-apps` (ver "Stack Airflow + Spark" abajo) — los mismos dos archivos son a la vez los scripts que corrés localmente y las aplicaciones que `spark-submit`/`SparkSubmitOperator` envían al cluster, sin copias ni versiones distintas.

Toda la lógica de Spark de cada paso vive **como procedimiento, dentro de una sola función `run(...)`** en `spark/read_queue.py` y en `spark/transform_messages.py` respectivamente — nada de partirla en funciones chicas por operación. La idea es que alguien pueda leer el script de arriba a abajo, con comentarios numerados por paso, y ver el pipeline completo tal cual se ejecuta, sin saltar entre funciones ni entre archivos. `src/` (a nivel de proyecto, hermana de `spark/`) quedó solo para lo que es configuración/infraestructura pura, sin lógica de negocio:

- **`src/schemas.py`**: schema explícito de Spark (StructType) para el body del mensaje y sus items — evita que Spark infiera `NullType` en columnas que a veces vienen todas en null, y es la fuente única de los nombres de campos (`EVENT_FIELDS`, `ITEM_FIELDS`, `CATEGORY_FIELD_CANDIDATES`).
- **`src/pseudo_json.py`**: parser puro (sin boto3 ni Spark) del formato tipo `toString()` de Java/Scala (`[{key=value, key2=value2}]`) que llega en el campo `items` — lo convierte a listas/dicts nativos de Python serializables como JSON real. Usado dentro de `spark/read_queue.py::run`.

Dentro de cada `run(...)`, el procedimiento queda así, paso a paso:

- `read_queue.py::run`: 0) arma su propia `SparkSession` (ver "Stack Airflow + Spark" para el detalle de esa config) — 1) un solo poll a SQS y parseo de cada mensaje, 2) records crudos a DataFrame con `RECORD_SCHEMA`, 3) lectura de la tabla existente y cálculo de cuántos `message_id` son nuevos, 4) upsert real (`unionByName` + `Window.partitionBy("message_id").orderBy(received_at.desc())` + quedarse con la fila 1 de cada partición), 5) escritura atómica a `.tmp` y reemplazo de la tabla. Sin loop propio: correrlo repetidamente (por ejemplo con un `schedule` en el DAG) es responsabilidad de quien lo invoca, no del script.
- `transform_messages.py::run`: 0) arma su propia `SparkSession` (mismo bloque de config que en `read_queue.py`), 1) explota `body.items` en una fila por item (evento sin items → una fila con item en null), 2) asigna `id = sha256(message_id + item_id)`, 3) pivotea `item_params` a columnas dinámicas, 4) hace el join de la fila base con las columnas pivoteadas, 5) upsert contra el CSV existente (mismo patrón `unionByName` + `Window` por `id`), 6) escribe un único CSV (Spark solo sabe escribir directorios de partes, así que se escribe a una carpeta temporal y se mueve la única parte generada), 7) imprime el conteo por `event_name` y por categoría.

Tanto el bloque de construcción de la `SparkSession` (paso 0) como el patrón de upsert (`unionByName` + `Window.partitionBy(key).orderBy(received_at.desc())` + `row_number == 1`) están deliberadamente duplicados entre los dos scripts en vez de extraídos a una función compartida (no existe `src/spark_session.py` ni ningún `get_spark()`) — se prefirió la duplicación a la abstracción para que cada script se pueda leer de punta a punta sin saltar a otro archivo, ni siquiera para algo tan básico como abrir la sesión de Spark.

Los scripts de `spark/` importan con `from src.<módulo> import ...`, pero viven en una subcarpeta — Python solo agrega al `sys.path` el directorio del propio script (`spark/`), no la raíz del proyecto donde está `src/`. Por eso hace falta `PYTHONPATH` apuntando a la raíz en los dos contextos donde corren: el `Makefile` local lo antepone (`PYTHONPATH=. venv/bin/python spark/read_queue.py`) y `docker-compose.yaml` lo fija a `/opt` (mismo mecanismo, ver abajo). Nunca correr estos scripts con un `python` que no tenga esa variable seteada — falla con `ModuleNotFoundError: No module named 'src'`.

## Comandos

Vía `Makefile` (usa directamente `venv/bin/python`, no hace falta activar el venv):

```bash
make install       # crea venv/ e instala requirements.txt
make check-creds   # valida que las credenciales de .env no estén expiradas
make read          # paso 1: poll a SQS + upsert en data/raw_messages/
make transform     # paso 2: flatten + upsert en output/items_flat.csv
make pipeline      # encadena read -> transform (default lógico del proyecto)
make clean         # borra data/, output/items_flat.csv y __pycache__
```

Equivalente manual (activando el venv; ojo con `PYTHONPATH`, ver nota arriba):

```bash
source venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=.

# Paso 1: leer una vez y hacer upsert en data/raw_messages/
python spark/read_queue.py

# Paso 1 a otra tabla
python spark/read_queue.py --table-path otra/ruta

# Paso 2: transformar (por defecto lee data/raw_messages/, escribe output/items_flat.csv)
python spark/transform_messages.py
python spark/transform_messages.py otra/ruta/parquet otro.csv
```

## Variables de entorno (`.env`)

`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (credenciales STS temporales — expiran, hay que refrescarlas), `AWS_DEFAULT_REGION`, `SQS_QUEUE_URL`, `SQS_DLQ_URL`.

**`.env` nunca debe commitearse** (ya está en `.gitignore`). Si las credenciales expiran, `read_queue.py` falla al autenticar contra SQS (`ExpiredToken`); hay que regenerarlas y actualizar el archivo. Se puede validar rápido con `aws sts get-caller-identity` usando esas mismas variables.

## Entorno local

Requiere Java (usa OpenJDK 11 instalado en la máquina) además del venv de Python — PySpark corre sobre la JVM local (`master("local[*]")`), sin cluster externo.

Si el venv se mueve o se renombra la carpeta del proyecto, `source venv/bin/activate` deja de apuntar bien (el activate script tiene la ruta absoluta hardcodeada) — hay que recrearlo con `python3 -m venv venv` desde la ruta final.

## Gotchas conocidos (pipeline local)

- Como `read_queue.py` no borra ni oculta mensajes, una cola con más mensajes en vuelo que `MAX_MESSAGES_PER_POLL` (10) puede necesitar varias corridas para verlos todos; no afecta la integridad de la tabla (upsert por `message_id`), solo cuántos mensajes nuevos aparecen en cada corrida.
- El parser de pseudo-JSON (`src/pseudo_json.py`) solo respeta anidamiento de `{}`, `[]`, `()`; no maneja comas o llaves dentro de un valor string entre comillas. Con el formato actual de los eventos no da problema, pero es el primer punto a revisar si aparecen valores de texto con comas literales.
- En `transform_messages.py`, si una clave de `item_params` coincide con el nombre de una columna ya existente (`message_id`, `id`, cualquier campo de item), el `join` del pivot puede generar columnas duplicadas o pisar una columna existente. No hay guardas para esto porque no se ha visto en los datos reales.
- `spark/read_queue.py` y `spark/transform_messages.py` calculan la raíz del proyecto como `dirname(dirname(__file__))` (dos niveles arriba, porque viven en `spark/`) — si algún día se anidan en una subcarpeta más, hay que ajustar ese cálculo.
- `data/` (tabla Parquet cruda) y `output/*.csv` están en `.gitignore` por ser datos generados, no código fuente. `messages.json` quedó del diseño anterior (JSON plano sin Spark) y ya no lo usa ningún script — se puede borrar cuando quieras.

## Stack Airflow + Spark (Docker)

`docker-compose.yaml` levanta **Apache Airflow 3.3.1** (CeleryExecutor) + **Apache Spark 4.1.3** standalone (1 master + 2 workers) + Postgres/Redis, calcado de un stack de referencia (`~/Escritorio/airflow-spark-stack`) pero **sin nada de Google Cloud** (sin providers, sin conectores GCS/BigQuery, sin `gcp/`). Pensado para correr el mismo ETL de arriba orquestado, no un proyecto aparte.

### Arquitectura

- **postgres** / **redis** — metadata DB de Airflow / broker de Celery.
- **spark-master** / **spark-worker** (x2) — imagen oficial `apache/spark` (sin imagen custom: los jobs son DataFrame/SQL puro, no usan `applyInPandas`), corren como `root` con `umask 000`.
- **airflow-apiserver / scheduler / dag-processor / triggerer / worker-1** — plano de control de Airflow 3.x, comparten config vía el YAML anchor `x-airflow-common`.
- **airflow-init** — migración de DB + usuario admin, corre una vez.

`spark/` (sin copia ni adaptación) se monta completa como `/opt/spark-apps` en los tres contenedores Spark-capaces (spark-master, spark-worker, y el driver dentro de `airflow-worker-1`). `src/` se monta como `/opt/src` con `PYTHONPATH=/opt` (mismo mecanismo que `PYTHONPATH=.` en local, ver arriba). `data/` y `output/` se montan como `/opt/spark-data` y `/opt/spark-output` — son las mismas carpetas que usa el pipeline local, así que **ambos modos comparten la misma tabla Parquet y el mismo CSV**, upsert incluido.

`dags/etl_pipeline.py` tiene un solo DAG (`etl_pipeline`, `schedule=None`, disparo manual) con dos `SparkSubmitOperator` encadenados (`conn_id="spark_default"`, que resuelve a `spark://spark-master:7077?deploy_mode=client` vía la env var `AIRFLOW_CONN_SPARK_DEFAULT`): `read_queue` (args `--table-path /opt/spark-data/raw_messages`) → `transform_messages` (args `/opt/spark-data/raw_messages /opt/spark-output/items_flat.csv`).

Cada script lee la env var `SPARK_MASTER_URL` al armar su `SparkSession` (paso 0 de `run()`): si está seteada (el stack la fija a `spark://spark-master:7077`), se conecta al cluster real; si no (host, `make pipeline`), sigue usando `local[*]`. Mismo bloque de código en los dos scripts, sin módulo compartido de por medio.

### Comandos (Makefile, prefijo `stack-`)

```bash
make stack-init             # primera vez: crea dirs, fija AIRFLOW_UID, migra DB, crea admin
make stack-up                # build + levanta todo el stack en background
make stack-ps                 # estado/salud de los servicios
make stack-logs                # sigue los logs de todo el stack
make stack-trigger              # dispara el DAG etl_pipeline
make stack-down                  # detiene todo
make stack-restart                 # down + up
make stack-clean                    # down -v (BORRA la metadata DB de Postgres)
make stack-reset                     # clean + init + up
make stack-flower                     # Flower (monitor de Celery) en :5555
make stack-airflow-shell                # shell dentro de airflow-apiserver
make stack-spark-shell                   # spark-shell interactivo contra el cluster
```

Airflow UI → http://localhost:8080 (`airflow`/`airflow` por defecto, en `.env`). Spark master UI → :8081, workers → :8082+. Validado de punta a punta: `make stack-trigger` corrió `etl_pipeline` completo (`read_queue` → `transform_messages`, ambas `success`) contra el cluster real.

### Variables de entorno nuevas (`.env` / `.env.example`)

Todo lo de Airflow/Spark/Postgres/Redis (versiones, puertos, límites de recursos, usuario admin) vive en el mismo `.env` que las credenciales AWS/SQS — ver `.env.example` para el detalle completo de cada variable. Solo hace falta esta sección si vas a levantar el stack; el pipeline local no la toca.

### Decisiones no obvias / troubleshooting ya resuelto

1. **`spark.master(...)` explícito en código pisa el `--master` de `spark-submit`.** Por eso el bloque de construcción de `SparkSession` en cada script nunca hardcodea `local[*]`: lee `SPARK_MASTER_URL` del entorno y arma el `.master(...)` correcto para cada contexto (ver arriba). Un job nuevo que llame a `SparkSession.builder` sin ese mismo patrón va a ignorar el cluster real y correr en modo local dentro del contenedor — no hacer eso.

2. **Primera corrida del DAG falló con `java.io.IOException: Failed to rename ... part-00000....snappy.parquet`** al escribir la tabla Parquet — mismo bug ya documentado en el stack de referencia (commit protocol v1 de Spark falla al renombrar sobre volúmenes bind-mounted de Docker, sin relación con permisos). **Fix aplicado en el bloque de `SparkSession` de cada script**: `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` + `spark.hadoop.fs.permissions.umask-mode=000`. No sacarlos de ninguno de los dos scripts.

3. **Un directorio `.tmp` a medio escribir de una corrida fallida puede quedar con dueño `50000:root` (AIRFLOW_UID) y permisos que el usuario del host no puede borrar.** No usar `sudo` a ciegas: como `spark-master`/`spark-worker` corren como `root` con el volumen montado, `docker compose exec --user root spark-master rm -rf /opt/spark-data/<carpeta>.tmp` lo limpia sin pedir contraseña del host.

4. **`spark-master`/`spark-worker` corren la imagen oficial `apache/spark`, sin `boto3` ni `python-dotenv`.** Por eso `read_queue.py` (necesita hablar con SQS) solo puede correr como driver dentro de `airflow-worker-1` (`deploy_mode=client`, que sí tiene `requirements-airflow.txt` instalado) — vía el DAG, no con `spark-submit` directo contra `spark-master`. `transform_messages.py` (DataFrame/SQL puro, sin libs de terceros) sí podría correr standalone contra `spark-master` si hiciera falta debuggear.

5. **`requirements.txt` (venv local, Spark 3.5.3, Java 11 del host) y `requirements-airflow.txt` (imagen Docker, Spark 4.1.3, Java 17 vía `Dockerfile`) son deliberadamente dos archivos separados con versiones de Spark distintas.** Unificarlas rompería el pipeline local: Spark 4.1.3 no arranca con Java 11 (`UnsupportedClassVersionError`), y este host solo tiene OpenJDK 11 instalado fuera de Docker.
