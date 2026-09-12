# nelo-test

Pipeline en dos pasos, sobre PySpark (local), para consumir eventos de analítica (estilo GA4 e-commerce) desde SQS y dejarlos aplanados en un CSV. Pensado para que cada paso se pueda enganchar luego como task de Airflow (cada uno expone una función `run(...)` de parámetros explícitos, separada del `main()` que solo parsea CLI).

## Flujo de datos

```
SQS (data-engineering-case-analytics-queue)
  → read_queue.py        → data/raw_messages/   (tabla Parquet, upsert por message_id)
  → transform_messages.py → output/items_flat.csv (upsert por id = sha256(message_id + item_id))
```

Toda la lógica de Spark (armar DataFrames, explotar/pivotear, upsert por ventana, escritura atómica) vive **directamente en `read_queue.py` y `transform_messages.py`**, en funciones chicas de un solo propósito dentro del mismo archivo — a propósito, para que alguien pueda leer un script de arriba a abajo y ver el pipeline completo sin saltar entre módulos. `src/` quedó solo para lo que es configuración/infraestructura pura, sin lógica de negocio:

- **`src/schemas.py`**: schema explícito de Spark (StructType) para el body del mensaje y sus items — evita que Spark infiera `NullType` en columnas que a veces vienen todas en null, y es la fuente única de los nombres de campos (`EVENT_FIELDS`, `ITEM_FIELDS`, `CATEGORY_FIELD_CANDIDATES`).
- **`src/pseudo_json.py`**: parser puro (sin boto3 ni Spark) del formato tipo `toString()` de Java/Scala (`[{key=value, key2=value2}]`) que llega en el campo `items` — lo convierte a listas/dicts nativos de Python serializables como JSON real. Usado por `read_queue.py::fetch_messages`.
- **`src/spark_session.py`**: builder único de `SparkSession` (`local[*]`), reutilizado por ambos scripts de la raíz.

Dentro de cada script, el pipeline queda visible como una secuencia de funciones:

- `read_queue.py`: `fetch_messages` (boto3) → `build_new_dataframe` (JSON crudo a DataFrame con `RECORD_SCHEMA`) → `load_existing_table` → `count_new_message_ids` / `upsert_by_message_id` (union + window por `received_at` desc, quedándose con la fila más reciente por `message_id`) → `write_table_atomic` (escribe a `.tmp` y recién al final reemplaza la tabla).
- `transform_messages.py`: `load_raw_table` → `explode_items` (una fila por item) → `with_row_id` (`id = sha256(message_id + item_id)`) → `pivot_item_params` → `build_flat_dataframe` (join de lo anterior) → `load_existing_csv` → `count_new_ids` / `upsert_by_id` (mismo patrón union + window que en `read_queue.py`, pero por `id`) → `write_csv_atomic` (Spark solo sabe escribir directorios de partes; esto arma un único `.csv` en `output/`) → `print_quick_analysis`.

`upsert_by_message_id` y `upsert_by_id` son deliberadamente el mismo patrón duplicado en los dos scripts (union + `Window.partitionBy(key).orderBy(received_at.desc())` + `row_number` == 1) en vez de una función compartida — se prefirió la duplicación a la abstracción para que cada script se pueda leer de forma autocontenida.

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
- El parser de pseudo-JSON (`src/pseudo_json.py`) solo respeta anidamiento de `{}`, `[]`, `()`; no maneja comas o llaves dentro de un valor string entre comillas. Con el formato actual de los eventos no da problema, pero es el primer punto a revisar si aparecen valores de texto con comas literales.
- En `transform_messages.py`, si una clave de `item_params` coincide con el nombre de una columna ya existente (`message_id`, `id`, cualquier campo de item), el `join` del pivot puede generar columnas duplicadas o pisar una columna existente. No hay guardas para esto porque no se ha visto en los datos reales.
- `data/` (tabla Parquet cruda) y `output/*.csv` están en `.gitignore` por ser datos generados, no código fuente. `messages.json` quedó del diseño anterior (JSON plano sin Spark) y ya no lo usa ningún script — se puede borrar cuando quieras.
