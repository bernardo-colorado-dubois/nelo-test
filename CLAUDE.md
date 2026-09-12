# nelo-test

Pipeline en tres pasos, sobre PySpark, para consumir eventos de analítica (estilo GA4 e-commerce) desde SQS y dejarlos aplanados en un CSV. Corre de dos formas:

1. **Local** (`make pipeline`): Spark `local[*]` sobre el venv del host, sin Docker. Ver "Comandos" abajo.
2. **Orquestado** (`make stack-up`): los mismos tres scripts, sin modificar, enviados como jobs al cluster de Spark standalone vía un DAG de Airflow. Ver "Stack Airflow + Spark (Docker)" más abajo.

Ninguno de los tres tiene `run()` ni `main()`: son procedimientos planos bajo `if __name__ == "__main__":` — nada más los llama ni hace falta reusarlos desde otro código Python, así que esas funciones no aportaban nada. Los tres corren igual como script local o como aplicación de `spark-submit`.

## Flujo de datos

```
SQS (data-engineering-case-analytics-queue)
  → spark/read_queue.py        → data/raw_messages/  (tabla Parquet, upsert por message_id)
  → spark/transform_messages.py → data/items_flat/     (tabla Parquet, upsert por id = sha256(message_id + item_id))
  → spark/export_csv.py          → output/items_flat.csv (volcado completo, sin merge)
```

`transform_messages.py` ya no escribe directo a CSV: escribe una tabla Parquet incremental (`data/items_flat/`), con el mismo patrón de upsert que `read_queue.py` usa para `data/raw_messages/` (unión + ventana + reemplazo atómico). `export_csv.py` es el único paso que toca `output/items_flat.csv` — lee la tabla ya deduplicada y la vuelca completa a un solo archivo; no hace merge de nada, así que no necesita saber nada de `message_id`/`id`, solo copiar el estado actual.

`spark/` es la carpeta que se monta tal cual dentro del stack de Docker como `/opt/spark-apps` (ver "Stack Airflow + Spark" abajo) — los mismos archivos son a la vez los scripts que corrés localmente y las aplicaciones que `spark-submit`/`SparkSubmitOperator` envían al cluster, sin copias ni versiones distintas.

Toda la lógica de Spark de cada paso vive **como procedimiento**, en los tres scripts de `spark/` — nada de partirla en funciones chicas por operación. La idea es que alguien pueda leer el script de arriba a abajo, con comentarios numerados por paso, y ver el pipeline completo tal cual se ejecuta, sin saltar entre funciones ni entre archivos. `src/` (a nivel de proyecto, hermana de `spark/`) quedó solo para lo que es configuración/infraestructura pura, sin lógica de negocio:

- **`src/schemas.py`**: schema explícito de Spark (StructType) para el body del mensaje y sus items — evita que Spark infiera `NullType` en columnas que a veces vienen todas en null, y es la fuente única de los nombres de campos (`EVENT_FIELDS`, `ITEM_FIELDS`, `CATEGORY_FIELD_CANDIDATES`).
- **`src/pseudo_json.py`**: parser puro (sin boto3 ni Spark) del formato tipo `toString()` de Java/Scala (`[{key=value, key2=value2}]`) que llega en el campo `items` — lo convierte a listas/dicts nativos de Python serializables como JSON real. Usado dentro de `spark/read_queue.py`.

El procedimiento queda así, paso a paso:

- `read_queue.py`: 0) arma su propia `SparkSession` (ver "Stack Airflow + Spark" para el detalle de esa config) — 1) un solo poll a SQS (`VisibilityTimeout=120`) y parseo de cada mensaje, 2) records crudos a DataFrame con `RECORD_SCHEMA`, 3) lectura de la tabla existente y cálculo de cuántos `message_id` son nuevos, 4) upsert real (`unionByName` + `Window.partitionBy("message_id").orderBy(received_at.desc())` + quedarse con la fila 1 de cada partición), 5) escritura atómica a `.tmp` y reemplazo de la tabla, 6) recién ahí, con los mensajes ya durables en Parquet, `delete_message_batch` los saca de la cola (hasta 10 por llamada, uno a uno con el tamaño del poll). Sin loop propio: correrlo repetidamente (por ejemplo con un `schedule` en el DAG) es responsabilidad de quien lo invoca, no del script.
- `transform_messages.py`: 0) arma su propia `SparkSession` (mismo bloque de config que en `read_queue.py`), 1) explota `body.items` en una fila por item (evento sin items → una fila con item en null), 2) asigna `id = sha256(message_id + item_id)`, 3) pivotea `item_params` a columnas dinámicas, 4) hace el join de la fila base con las columnas pivoteadas, 5) lee la tabla `items_flat` existente y calcula cuántos `id` son nuevos, 6) upsert (mismo patrón `unionByName` + `Window` por `id` que `read_queue.py`) — el resultado se cachea y materializa (`.cache()` + `.count()`) **antes** de escribir, porque su plan todavía depende de leer `items_flat/` y el paso siguiente borra esa carpeta (ver punto 7 de "Decisiones no obvias" para el porqué exacto), 7) escritura atómica a `.tmp` y reemplazo de la tabla, 8) imprime el conteo por `event_name` y por categoría.
- `export_csv.py`: 0) arma su propia `SparkSession`, 1) lee `items_flat/` tal cual (ya viene deduplicada), 2) la escribe completa como un único CSV (Spark solo sabe escribir directorios de partes, así que escribe a una carpeta temporal y mueve la única parte generada al nombre final). No hace upsert ni sabe de `id` — es un volcado, no un merge.

El bloque de construcción de la `SparkSession` (paso 0) y el patrón de upsert (`unionByName` + `Window.partitionBy(key).orderBy(received_at.desc())` + `row_number == 1`) están deliberadamente duplicados entre `read_queue.py` y `transform_messages.py` en vez de extraídos a una función compartida (no existe `src/spark_session.py` ni ningún `get_spark()`) — se prefirió la duplicación a la abstracción para que cada script se pueda leer de punta a punta sin saltar a otro archivo, ni siquiera para algo tan básico como abrir la sesión de Spark. `export_csv.py` reutiliza el mismo bloque de `SparkSession` pero no el de upsert, porque no hace ninguno.

Los scripts de `spark/` importan con `from src.<módulo> import ...`, pero viven en una subcarpeta — Python solo agrega al `sys.path` el directorio del propio script (`spark/`), no la raíz del proyecto donde está `src/`. Por eso hace falta `PYTHONPATH` apuntando a la raíz en los dos contextos donde corren: el `Makefile` local lo antepone (`PYTHONPATH=. venv/bin/python spark/read_queue.py`) y `docker-compose.yaml` lo fija a `/opt` (mismo mecanismo, ver abajo). Nunca correr estos scripts con un `python` que no tenga esa variable seteada — falla con `ModuleNotFoundError: No module named 'src'`.

## Comandos

Vía `Makefile` (usa directamente `venv/bin/python`, no hace falta activar el venv):

```bash
make install       # crea venv/ e instala requirements.txt
make check-creds   # valida que las credenciales de .env no estén expiradas
make read          # paso 1: poll a SQS + upsert en data/raw_messages/
make transform     # paso 2: flatten + upsert en data/items_flat/
make export        # paso 3: vuelca data/items_flat/ a output/items_flat.csv
make pipeline      # encadena read -> transform -> export (default lógico del proyecto)
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

# Paso 2: transformar (por defecto lee data/raw_messages/, escribe data/items_flat/)
python spark/transform_messages.py
python spark/transform_messages.py otra/ruta/parquet otra/salida/parquet

# Paso 3: exportar (por defecto lee data/items_flat/, escribe output/items_flat.csv)
python spark/export_csv.py
python spark/export_csv.py otra/salida/parquet otro.csv
```

## Variables de entorno (`.env`)

`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (credenciales STS temporales — expiran, hay que refrescarlas), `AWS_DEFAULT_REGION`, `SQS_QUEUE_URL`, `SQS_DLQ_URL`.

**`.env` nunca debe commitearse** (ya está en `.gitignore`). Si las credenciales expiran, `read_queue.py` falla al autenticar contra SQS (`ExpiredToken`); hay que regenerarlas y actualizar el archivo. Se puede validar rápido con `aws sts get-caller-identity` usando esas mismas variables.

## Entorno local

Requiere Java (usa OpenJDK 11 instalado en la máquina) además del venv de Python — PySpark corre sobre la JVM local (`master("local[*]")`), sin cluster externo.

Si el venv se mueve o se renombra la carpeta del proyecto, `source venv/bin/activate` deja de apuntar bien (el activate script tiene la ruta absoluta hardcodeada) — hay que recrearlo con `python3 -m venv venv` desde la ruta final.

`requirements.txt` también trae `apache-airflow==3.3.1` + `apache-airflow-providers-apache-spark==6.3.1` (mismas versiones que `requirements-airflow.txt`/`.env`) — no para correr el pipeline (eso sigue siendo `boto3`/`pyspark` puro), sino para que el IDE resuelva los imports de `dags/etl_pipeline.py` y se pueda hacer `from airflow import DAG` o correr `airflow` localmente sin entrar al contenedor. Instalar con el constraints file oficial de Airflow para evitar conflictos de resolución (fija versiones de `boto3`/`python-dotenv` distintas a las que hubiera puesto pip solo):
```bash
pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"
```
(usar `constraints-3.10.txt` en vez de `constraints-3.12.txt` si el venv corre Python 3.10). Ver punto 8 de "Decisiones no obvias" del stack para el problema real que esto introduce (`pyspark-client`) y su fix.

## Gotchas conocidos (pipeline local)

- `read_queue.py` **sí consume la cola**: hace `delete_message_batch` de los mensajes recién después de que el upsert en `data/raw_messages/` ya se escribió con éxito — si la escritura falla antes de llegar a ese paso, los mensajes nunca se borran y vuelven a quedar visibles solos a los 120s (`VisibilityTimeout`) para que la próxima corrida los reintente, sin perder nada (el upsert por `message_id` los deduplica igual si llegan dos veces). Con `MAX_MESSAGES_PER_POLL=10` (el máximo que permite la API de SQS por `receive_message`), cada corrida procesa y borra hasta 10 mensajes; una cola con más en vuelo simplemente necesita más corridas para vaciarse.
- El parser de pseudo-JSON (`src/pseudo_json.py`) solo respeta anidamiento de `{}`, `[]`, `()`; no maneja comas o llaves dentro de un valor string entre comillas. Con el formato actual de los eventos no da problema, pero es el primer punto a revisar si aparecen valores de texto con comas literales.
- En `transform_messages.py`, si una clave de `item_params` coincide con el nombre de una columna ya existente (`message_id`, `id`, cualquier campo de item), el `join` del pivot puede generar columnas duplicadas o pisar una columna existente. No hay guardas para esto porque no se ha visto en los datos reales.
- Los tres scripts de `spark/` calculan la raíz del proyecto como `dirname(dirname(__file__))` (dos niveles arriba, porque viven en `spark/`) — si algún día se anidan en una subcarpeta más, hay que ajustar ese cálculo.
- `data/` (tablas Parquet: `raw_messages/` y `items_flat/`) y `output/*.csv` están en `.gitignore` por ser datos generados, no código fuente. `messages.json` quedó del diseño anterior (JSON plano sin Spark) y ya no lo usa ningún script — se puede borrar cuando quieras.

## Stack Airflow + Spark (Docker)

`docker-compose.yaml` levanta **Apache Airflow 3.3.1** (CeleryExecutor) + **Apache Spark 4.1.3** standalone (1 master + 2 workers) + Postgres/Redis, calcado de un stack de referencia (`~/Escritorio/airflow-spark-stack`) pero **sin nada de Google Cloud** (sin providers, sin conectores GCS/BigQuery, sin `gcp/`). Pensado para correr el mismo ETL de arriba orquestado, no un proyecto aparte.

### Arquitectura

- **postgres** / **redis** — metadata DB de Airflow / broker de Celery.
- **spark-master** / **spark-worker** (x2) — imagen oficial `apache/spark` (sin imagen custom: los jobs son DataFrame/SQL puro, no usan `applyInPandas`), corren como `root` con `umask 000`.
- **airflow-apiserver / scheduler / dag-processor / triggerer / worker-1** — plano de control de Airflow 3.x, comparten config vía el YAML anchor `x-airflow-common`.
- **airflow-init** — migración de DB + usuario admin, corre una vez.

`spark/` (sin copia ni adaptación) se monta completa como `/opt/spark-apps` en los tres contenedores Spark-capaces (spark-master, spark-worker, y el driver dentro de `airflow-worker-1`). `src/` se monta como `/opt/src` con `PYTHONPATH=/opt` (mismo mecanismo que `PYTHONPATH=.` en local, ver arriba). `data/` y `output/` se montan como `/opt/spark-data` y `/opt/spark-output` — son las mismas carpetas que usa el pipeline local, así que **ambos modos comparten la misma tabla Parquet y el mismo CSV**, upsert incluido.

`dags/etl_pipeline.py` tiene un solo DAG (`etl_pipeline`, `schedule=None`, disparo manual) con tres `SparkSubmitOperator` encadenados (`conn_id="spark_default"`, que resuelve a `spark://spark-master:7077?deploy_mode=client` vía la env var `AIRFLOW_CONN_SPARK_DEFAULT`): `read_queue` (args `--table-path /opt/spark-data/raw_messages --max-messages-per-poll 10 --wait-time-seconds 20`) → `transform_messages` (args `/opt/spark-data/raw_messages /opt/spark-data/items_flat`) → `export_csv` (args `/opt/spark-data/items_flat /opt/spark-output/items_flat.csv`). `MAX_MESSAGES_PER_POLL`/`WAIT_TIME_SECONDS` son parámetros operativos del poll a SQS que decide el DAG, no constantes fijas en `read_queue.py` — el único valor que sigue hardcodeado ahí es `VisibilityTimeout=120`, porque es un invariante de diseño (cubre el tiempo de la propia corrida hasta el borrado, ver punto 9 de "Decisiones no obvias") y no algo que el DAG deba poder cambiar.

Cada script lee la env var `SPARK_MASTER_URL` al armar su `SparkSession` (paso 0): si está seteada (el stack la fija a `spark://spark-master:7077`), se conecta al cluster real; si no (host, `make pipeline`), sigue usando `local[*]`. Mismo bloque de código en los dos scripts, sin módulo compartido de por medio.

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

Airflow UI → http://localhost:8080 (`airflow`/`airflow` por defecto, en `.env`). Spark master UI → :8081, workers → :8082+. Validado de punta a punta: `make stack-trigger` corrió `etl_pipeline` completo (`read_queue` → `transform_messages` → `export_csv`, las tres `success`) contra el cluster real.

### Variables de entorno nuevas (`.env` / `.env.example`)

Todo lo de Airflow/Spark/Postgres/Redis (versiones, puertos, límites de recursos, usuario admin) vive en el mismo `.env` que las credenciales AWS/SQS — ver `.env.example` para el detalle completo de cada variable. Solo hace falta esta sección si vas a levantar el stack; el pipeline local no la toca.

### Decisiones no obvias / troubleshooting ya resuelto

1. **`spark.master(...)` explícito en código pisa el `--master` de `spark-submit`.** Por eso el bloque de construcción de `SparkSession` en cada script nunca hardcodea `local[*]`: lee `SPARK_MASTER_URL` del entorno y arma el `.master(...)` correcto para cada contexto (ver arriba). Un job nuevo que llame a `SparkSession.builder` sin ese mismo patrón va a ignorar el cluster real y correr en modo local dentro del contenedor — no hacer eso.

2. **Primera corrida del DAG falló con `java.io.IOException: Failed to rename ... part-00000....snappy.parquet`** al escribir la tabla Parquet — mismo bug ya documentado en el stack de referencia (commit protocol v1 de Spark falla al renombrar sobre volúmenes bind-mounted de Docker, sin relación con permisos). **Fix aplicado en el bloque de `SparkSession` de cada script**: `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` + `spark.hadoop.fs.permissions.umask-mode=000`. No sacarlos de ninguno de los dos scripts.

3. **Un directorio `.tmp` a medio escribir de una corrida fallida puede quedar con dueño `50000:root` (AIRFLOW_UID) y permisos que el usuario del host no puede borrar.** No usar `sudo` a ciegas: como `spark-master`/`spark-worker` corren como `root` con el volumen montado, `docker compose exec --user root spark-master rm -rf /opt/spark-data/<carpeta>.tmp` lo limpia sin pedir contraseña del host.

4. **`spark-master`/`spark-worker` corren la imagen oficial `apache/spark`, sin `boto3` ni `python-dotenv`.** Por eso `read_queue.py` (necesita hablar con SQS) solo puede correr como driver dentro de `airflow-worker-1` (`deploy_mode=client`, que sí tiene `requirements-airflow.txt` instalado) — vía el DAG, no con `spark-submit` directo contra `spark-master`. `transform_messages.py` y `export_csv.py` (DataFrame/SQL puro, sin libs de terceros) sí podrían correr standalone contra `spark-master` si hiciera falta debuggear.

5. **`requirements.txt` (venv local, Spark 3.5.3, Java 11 del host) y `requirements-airflow.txt` (imagen Docker, Spark 4.1.3, Java 17 vía `Dockerfile`) son deliberadamente dos archivos separados con versiones de Spark distintas.** Unificarlas rompería el pipeline local: Spark 4.1.3 no arranca con Java 11 (`UnsupportedClassVersionError`), y este host solo tiene OpenJDK 11 instalado fuera de Docker.

6. **Nunca pre-completar `AIRFLOW_UID` en `.env`/`.env.example` con un valor fijo.** `airflow-init` corre `chown -R "${AIRFLOW_UID}:0" /opt/airflow/{logs,dags,plugins,config}` sobre esas carpetas bind-mounted — si `AIRFLOW_UID` ya tiene un valor cuando corre `make stack-init` (el check del Makefile es `grep -q "^AIRFLOW_UID=" .env`, no compara contra el UID real), ese `chown` se aplica tal cual y el usuario del host deja de poder editar `dags/*.py` (`Permission denied`, visto en vivo tras dejar `AIRFLOW_UID=50000` en `.env`). `.env.example` deja la línea comentada (`# AIRFLOW_UID=`) para que `stack-init` la complete solo con `id -u` la primera vez. Si ya pasó, arreglar con `docker compose run --rm --user root --no-deps --entrypoint /bin/bash airflow-apiserver -c "chown -R $(id -u):$(id -g) /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/config"` y corregir `AIRFLOW_UID` en `.env` al UID real.

7. **`transform_messages.py` falló en la segunda corrida con `SparkFileNotFoundException: ... part-00000....snappy.parquet does not exist`, solo cuando ya existía `items_flat/` de una corrida previa.** Causa: `merged_df` es perezoso y su plan incluye leer `items_flat/` (vía `existing_df`, para el upsert); el paso de escritura borra y reemplaza esa misma carpeta (mismo patrón atómico que `read_queue.py`). Cualquier acción sobre `merged_df` **después** de la escritura (el `count()` para el log, los `groupBy` del análisis) reevalúa ese plan perezoso contra archivos que el propio `rename` ya borró/reemplazó. **Fix**: `.cache()` sobre `merged_df` + un `.count()` que lo materialice, ambos **antes** de escribir — todo lo que se calcule después (el mismo `count()` guardado, el análisis) lee del cache, no reevalúa el plan. `read_queue.py` nunca tuvo este problema porque no vuelve a tocar `merged_df` después de escribir. Si se agrega un job nuevo que lea-modifique-reemplace su propia fuente de lectura, aplicar el mismo patrón (cachear antes de escribir) o evitar cualquier acción posterior a la escritura sobre ese DataFrame.

8. **Instalar `apache-airflow-providers-apache-spark` en el venv local rompe `import pyspark`** (`ImportError: cannot import name '_with_origin' from 'pyspark.errors.utils'`, o directamente `pyspark.__version__` reportando una versión que nunca pediste). Causa: ese provider trae `pyspark-client` (thin client de Spark Connect) como dependencia, que instala archivos bajo el mismo namespace `pyspark/` y pisa/mezcla módulos del `pyspark` real — queda un híbrido roto de los dos paquetes. **Fix** (ya aplicado en `make install`): `pip uninstall -y pyspark pyspark-client` seguido de `pip install pyspark==3.5.3` — un `--force-reinstall --no-deps` normal **no alcanza**, porque no borra los archivos extra que dejó `pyspark-client` fuera del manifiesto del propio `pyspark`. Si `pip install -r requirements.txt` se vuelve a correr suelto (sin pasar por `make install`), repetir el uninstall+reinstall a mano.

9. **`read_queue.py` dejó de ser de solo lectura a propósito, una vez confirmado en varias corridas (local y contra el cluster) que el upsert por `message_id` es idempotente.** Ahora borra los mensajes de la cola real (`delete_message_batch`, hasta 10 por llamada) — es una acción irreversible sobre infraestructura compartida (AWS), no algo a tomar a la ligera si se vuelve a tocar este script. El orden importa: el `receive_message` pasó de `VisibilityTimeout=0` a `120` (oculta el mensaje a otros consumidores mientras dura la corrida) y el borrado ocurre **después** de la escritura atómica en `data/raw_messages/`, nunca antes — si la escritura falla, el mensaje ni se borra ni queda "perdido": vuelve a quedar visible solo a los 120s para que la corrida siguiente lo reintente, y el upsert lo deduplica si ya se había escrito antes de que fallara el borrado. `MAX_MESSAGES_PER_POLL=10` no es casualidad: es el máximo que acepta tanto `receive_message` como `delete_message_batch` en una sola llamada de la API de SQS, así que un poll y su borrado correspondiente siempre caben en una request de cada lado.
