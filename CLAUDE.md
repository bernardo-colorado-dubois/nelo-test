# nelo-test

Pipeline en dos pasos, sobre PySpark (local), para consumir eventos de analítica (estilo GA4 e-commerce) desde SQS y dejarlos aplanados en un CSV. Pensado para que cada paso se pueda enganchar luego como task de Airflow (cada uno expone una función `run(...)` de parámetros explícitos, separada del `main()` que solo parsea CLI).

## Flujo de datos

```
SQS (data-engineering-case-analytics-queue)
  → read_queue.py        → data/raw_messages/   (tabla Parquet, upsert por message_id)
  → transform_messages.py → output/items_flat.csv (upsert por id = sha256(message_id + item_id))
```

`read_queue.py` y `transform_messages.py` en la raíz son orquestadores delgados: solo leen configuración (env vars, argv), arman la `SparkSession` y llaman a las funciones de `src/`. Cada uno expone un `run(...)` de parámetros explícitos separado del `main()` (que solo parsea CLI), para que más adelante Airflow pueda invocar la función directamente sin pasar por `sys.argv`.

### `src/`

- **`schemas.py`**: schema explícito de Spark (StructType) para el body del mensaje y sus items — evita que Spark infiera `NullType` en columnas que a veces vienen todas en null, y es la fuente única de los nombres de campos (`EVENT_FIELDS`, `ITEM_FIELDS`, `CATEGORY_FIELD_CANDIDATES`) usados por el resto del código.
- **`pseudo_json.py`**: parser puro (sin boto3 ni Spark) del formato tipo `toString()` de Java/Scala (`[{key=value, key2=value2}]`) que llega en el campo `items` — lo convierte a listas/dicts nativos de Python serializables como JSON real.
- **`queue_reader.py`**: todo lo específico de SQS — cliente boto3, `fetch_messages` (poll de solo lectura, `VisibilityTimeout=0`, nunca `delete_message`), `records_to_dataframe` (arma el DataFrame de Spark con `RECORD_SCHEMA`) y `upsert_messages` (upsert contra la tabla Parquet: si el `message_id` ya existe se reemplaza por la lectura más reciente según `received_at`, si no existe se agrega).
- **`flatten.py`**: explota `body.items` (una fila por item; eventos sin items quedan como una fila con campos de item en null), pivotea `item_params` (`[{key, value}, ...]`) a columnas dinámicas, asigna `id = sha256(message_id + item_id)` a cada fila, y hace `upsert_flat_csv` contra `output/items_flat.csv` (mismo criterio de upsert: reemplaza por `id` si ya existía, agrega si es nuevo). El upsert reescribe el archivo completo — no es un `append` ciego a disco, es un merge+overwrite que da el mismo resultado idempotente.
- **`spark_io.py`**: funciones compartidas de upsert (`upsert_by_key`, `count_new_keys`) y de escritura atómica (`write_parquet_atomic`, `write_single_csv_atomic` — escriben a un directorio temporal y recién al final reemplazan el destino, para no dejar la tabla/CSV a medio escribir ni pisar la fuente mientras Spark todavía la está leyendo). Reutilizadas tanto por `queue_reader.py` como por `flatten.py`.
- **`spark_session.py`**: builder único de `SparkSession` (`local[*]`), reutilizado por ambos scripts de la raíz.

Los scripts de la raíz importan con `from src.<módulo> import ...`; funciona sin instalar el paquete porque Python agrega el directorio del script (la raíz del proyecto) a `sys.path` automáticamente — hay que seguir corriendo los scripts desde la raíz (`python read_queue.py`, no `python src/../read_queue.py` desde otro cwd sin ajustar `PYTHONPATH`).

## Comandos

Vía `Makefile` (usa directamente `venv/bin/python`, no hace falta activar el venv):

```bash
make install       # crea venv/ e instala requirements.txt
make check-creds   # valida que las credenciales de .env no estén expiradas
make read          # paso 1: poll a SQS + upsert en data/raw_messages/
make transform     # paso 2: flatten + upsert en output/items_flat.csv
make pipeline      # encadena read -> transform (default lógico del proyecto)
make loop          # paso 1 en loop continuo (Ctrl+C para cortar)
make clean         # borra data/, output/items_flat.csv y __pycache__
```

Equivalente manual (activando el venv):

```bash
source venv/bin/activate
pip install -r requirements.txt

# Paso 1: leer una vez y hacer upsert en data/raw_messages/
python read_queue.py

# Paso 1 en loop continuo (long polling, WaitTimeSeconds=20)
python read_queue.py --loop

# Paso 1 a otra tabla
python read_queue.py --table-path otra/ruta

# Paso 2: transformar (por defecto lee data/raw_messages/, escribe output/items_flat.csv)
python transform_messages.py
python transform_messages.py otra/ruta/parquet otro.csv
```

## Variables de entorno (`.env`)

`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (credenciales STS temporales — expiran, hay que refrescarlas), `AWS_DEFAULT_REGION`, `SQS_QUEUE_URL`, `SQS_DLQ_URL`.

**`.env` nunca debe commitearse** (ya está en `.gitignore`). Si las credenciales expiran, `read_queue.py` falla al autenticar contra SQS (`ExpiredToken`); hay que regenerarlas y actualizar el archivo. Se puede validar rápido con `aws sts get-caller-identity` usando esas mismas variables.

## Entorno local

Requiere Java (usa OpenJDK 11 instalado en la máquina) además del venv de Python — PySpark corre sobre la JVM local (`master("local[*]")`), sin cluster externo.

Si el venv se mueve o se renombra la carpeta del proyecto, `source venv/bin/activate` deja de apuntar bien (el activate script tiene la ruta absoluta hardcodeada) — hay que recrearlo con `python3 -m venv venv` desde la ruta final.

## Gotchas conocidos

- Como `read_queue.py` no borra ni oculta mensajes, correr `--loop` cuando la cola tiene más mensajes en vuelo que `MAX_MESSAGES_PER_POLL` (10) puede traer lotes parcialmente repetidos entre polls; no afecta la integridad de la tabla (upsert por `message_id`), solo la latencia para ver mensajes nuevos.
- El parser de pseudo-JSON (`split_top_level` / `parse_pseudo_json` en `read_queue.py`) solo respeta anidamiento de `{}`, `[]`, `()`; no maneja comas o llaves dentro de un valor string entre comillas. Con el formato actual de los eventos no da problema, pero es el primer punto a revisar si aparecen valores de texto con comas literales.
- En `transform_messages.py`, si una clave de `item_params` coincide con el nombre de una columna ya existente (`message_id`, `id`, cualquier campo de item), el `join` del pivot puede generar columnas duplicadas o pisar una columna existente. No hay guardas para esto porque no se ha visto en los datos reales.
- `data/` (tabla Parquet cruda) y `output/*.csv` están en `.gitignore` por ser datos generados, no código fuente. `messages.json` quedó del diseño anterior (JSON plano sin Spark) y ya no lo usa ningún script — se puede borrar cuando quieras.
