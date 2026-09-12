def split_top_level(text):
  """Divide un string por comas que están fuera de {}, [] o ()."""
  parts = []
  current = ""
  depth = 0
  for char in text:
    if char in "{[(":
      depth += 1
      current += char
    elif char in "}])":
      depth -= 1
      current += char
    elif char == "," and depth == 0:
      parts.append(current)
      current = ""
    else:
      current += char
  if current.strip():
    parts.append(current)
  return [p.strip() for p in parts]


def parse_scalar(text):
  text = text.strip()
  if text in ("null", "(not set)", ""):
    return None
  if text in ("true", "false"):
    return text == "true"
  try:
    return int(text)
  except ValueError:
    pass
  try:
    return float(text)
  except ValueError:
    pass
  return text


def parse_pseudo_json(text):
  """
  Convierte el formato tipo Java/Scala toString() ("[{key=value, key2=value2}]")
  que llega en campos como 'items' a estructuras nativas de Python
  (list/dict) serializables como JSON real.
  """
  text = text.strip()

  if text.startswith("[") and text.endswith("]"):
    inner = text[1:-1]
    return [parse_pseudo_json(item) for item in split_top_level(inner)]

  if text.startswith("{") and text.endswith("}"):
    inner = text[1:-1]
    result = {}
    for pair in split_top_level(inner):
      key, sep, value = pair.partition("=")
      if sep:
        result[key.strip()] = parse_pseudo_json(value.strip())
    return result

  return parse_scalar(text)


def expand_nested_fields(body, nested_fields):
  if not isinstance(body, dict):
    return body
  for field in nested_fields:
    value = body.get(field)
    if isinstance(value, str) and value:
      try:
        body[field] = parse_pseudo_json(value)
      except Exception:
        pass  # deja el valor original si no se pudo parsear
  return body
