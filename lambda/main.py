import json
import os
import re
import time
import boto3

# =========================
# Config
# =========================
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://YOUR_BUCKET_HERE/athena-results/")
DEFAULT_DB = os.getenv("DEFAULT_DB", "YOUR_DATABASE_HERE")
DEFAULT_WG = os.getenv("DEFAULT_WG", "YOUR_WORKGROUP_HERE")
MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))

athena = boto3.client("athena")

# Apenas SELECT (sem DDL/DML)
SELECT_ONLY = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)
BANNED = {"insert", "update", "delete", "merge", "create", "drop", "alter", "grant", "revoke"}

# =========================
# Helpers
# =========================
def _is_select(sql: str) -> bool:
    if not SELECT_ONLY.match(sql or ""):
        return False
    low = (sql or "").lower()
    # evita palavras banidas para fora de comentários
    return not any(f" {kw} " in low or low.startswith(f"{kw} ") for kw in BANNED)

def _ensure_limit(sql: str, max_rows: int = 1000) -> str:
    """
    - Remove ';' final
    - Se já houver LIMIT no fim, mantém (ou reduz para max_rows)
    - Caso não haja, adiciona LIMIT max_rows
    """
    s = (sql or "").rstrip()
    # remove ; final (e espaços)
    s = s.rstrip(";").rstrip()

    # procura LIMIT no fim da query (opcional OFFSET)
    m = re.search(r"(?is)\blimit\b\s+(\d+)(?:\s+offset\s+\d+)?\s*$", s)
    if m:
        try:
            lim = int(m.group(1))
            if lim > max_rows:
                # substitui por LIMIT max_rows
                s = re.sub(
                    r"(?is)\blimit\b\s+\d+(?:\s+offset\s+\d+)?\s*$",
                    f"LIMIT {max_rows}",
                    s
                )
        except ValueError:
            # se algo der errado no parse, apenas retorna s
            return s
        return s

    # não tinha LIMIT -> adiciona
    return f"{s}\nLIMIT {max_rows}"

def _extract_body(event: dict) -> dict:
    # Primeiro tenta o formato mais comum: parameters (dict)
    body = event.get("parameters")
    if isinstance(body, dict) and body:
        return body

    # Depois tenta o formato "body" bruto
    if isinstance(body, str):
        try:
            return json.loads(body or "{}")
        except json.JSONDecodeError:
            pass

    # Novo formato: dentro de requestBody -> content -> application/json -> properties[]
    req_body = event.get("requestBody", {}).get("content", {}).get("application/json", {})
    if "properties" in req_body and isinstance(req_body["properties"], list):
        parsed = {}
        for prop in req_body["properties"]:
            name = prop.get("name")
            value = prop.get("value")
            if name:
                parsed[name] = value
        if parsed:
            return parsed

    # Se nada encontrado, retorna dict vazio
    return {}

def _resolve_envelope_fields(event: dict):
    """
    Extrai actionGroup, apiPath e httpMethod do evento.
    Mantém defaults seguros caso não venham preenchidos.
    """
    rc = event.get("requestContext") or {}
    action_group = (
        event.get("actionGroup")
        or event.get("actionGroupName")
        or rc.get("actionGroup")
        or "ask-bnb-sql-exec"   # nome amigável; não precisa bater 100% com o título, mas ajuda nos logs
    )
    api_path = event.get("apiPath") or rc.get("apiPath") or "/run-sql"
    http_method = event.get("httpMethod") or rc.get("httpMethod") or "POST"
    return action_group, api_path, http_method

def _wrap_response(payload: dict, event: dict, status: int = 200) -> dict:
    """
    Constrói o envelope exigido por Bedrock Agents para Action Groups.
    IMPORTANTE: `responseBody.application/json.body` deve ser STRING.
    """
    action_group, api_path, http_method = _resolve_envelope_fields(event)
    return {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": action_group,
            "apiPath": api_path,
            "httpMethod": http_method,
            "httpStatusCode": status,
            "responseBody": {
                "application/json": {
                    "body": json.dumps(payload, ensure_ascii=False)
                }
            }
        }
    }

def _ok(columns, rows, bytes_scanned, event):
    payload = {
        "columns": columns or [],
        "rows": rows or [],
        "bytes_scanned": int(bytes_scanned or 0),
    }
    print(payload)  # útil nos logs do CloudWatch
    return _wrap_response(payload, event, 200)

def _err(event, status: int = 200):
    # Mantém compatível com o schema do OpenAPI (mesmo em erro)
    payload = {"columns": [], "rows": [], "bytes_scanned": 0}
    print({"error": True, "payload_returned": payload})
    return _wrap_response(payload, event, status)

# =========================
# Handler
# =========================
def lambda_handler(event, context):
    try:
        print("Evento recebido:", json.dumps(event, default=str))

        body = _extract_body(event)
        sql = body.get("sql", "")
        database = body.get("database", DEFAULT_DB)
        workgroup = body.get("workgroup", DEFAULT_WG)
        max_wait = int(body.get("max_wait_seconds", 25))

        # Garante apenas SELECT
        if not _is_select(sql):
            return _err(event)

        # Garante LIMIT
        sql = _ensure_limit(sql, MAX_ROWS)

        print("sql: ", sql)

        # Executa no Athena
        qexec = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup,
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        qid = qexec["QueryExecutionId"]

        # Espera completar (até max_wait)
        start = time.time()
        while True:
            qe = athena.get_query_execution(QueryExecutionId=qid)
            state = qe["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.time() - start > max_wait:
                # Timeout "limpo": ainda assim retorna schema do OpenAPI
                print(f"Timeout aguardando Athena ({max_wait}s). Cancelando {qid}.")
                try:
                    athena.stop_query_execution(QueryExecutionId=qid)
                except Exception as _:
                    pass
                return _err(event)
            time.sleep(0.6)

        if state != "SUCCEEDED":
            print(f"Query {qid} terminou em estado {state}.")
            return _err(event)

        stats = qe["QueryExecution"].get("Statistics", {})
        bytes_scanned = stats.get("DataScannedInBytes", 0)

        # Coleta resultados (Athena retorna header na primeira linha)
        res = athena.get_query_results(QueryExecutionId=qid, MaxResults=min(MAX_ROWS, 1000))
        cols = [c["Label"] for c in res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]

        rows = []
        for i, r in enumerate(res["ResultSet"]["Rows"]):
            if i == 0:
                continue  # pula header
            row = []
            for f in r.get("Data", []):
                val = f.get("VarCharValue") if f else None
                row.append("" if val is None else str(val))
            rows.append(row)

        return _ok(cols, rows, bytes_scanned, event)

    except Exception as e:
        print("Erro na Lambda:", str(e))
        return _err(event)
