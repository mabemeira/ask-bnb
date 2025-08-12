import os
import json
import time
import uuid
import pandas as pd
import streamlit as st
import boto3
from botocore.config import Config

# -----------------------------
# Configuração da página
# -----------------------------
st.set_page_config(page_title="Ask-BNB", layout="wide")

st.title("Ask-BNB – Interface Streamlit")

with st.sidebar:
    st.header("Config")
    AWS_REGION = st.text_input("AWS Region", os.getenv("AWS_REGION", "us-east-2"))
    AGENT_ID = st.text_input("Agent ID", os.getenv("AGENT_ID", "YOUR_AGENT_ID_HERE"))
    AGENT_ALIAS_ID = st.text_input("Agent Alias ID", os.getenv("AGENT_ALIAS_ID", "YOUR_AGENT_ALIAS_ID_HERE"))
    st.caption("Edite acima ou defina variáveis de ambiente: AWS_REGION, AGENT_ID, AGENT_ALIAS_ID.")
    st.divider()
    st.markdown("**Dicas**")
    st.caption("- O agente deve ter um action group que executa o SQL no Athena e retorna JSON com `sql`, `rows`, `columns`.")

# -----------------------------
# Estado da sessão
# -----------------------------
if "history" not in st.session_state:
    st.session_state.history = []  # cada item: {"role": "user"/"assistant", "content": "..."}
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

# -----------------------------
# Helper: cliente Bedrock Agents Runtime
# -----------------------------
def get_bedrock_client(region_name: str):
    cfg = Config(retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client("bedrock-agent-runtime", region_name=region_name, config=cfg)

# -----------------------------
# Helper: consumir stream do Bedrock
# -----------------------------
def invoke_agent_stream(prompt: str, agent_id: str, alias_id: str, region: str, session_id: str) -> str:
    """
    Chama o Bedrock Agent e concatena o stream de 'completion' em texto.
    Retorna a resposta completa como string (que pode conter JSON).
    """
    client = get_bedrock_client(region)
    response = client.invoke_agent(
        agentId=agent_id,
        agentAliasId=alias_id,
        sessionId=session_id,
        inputText=prompt,
    )

    # A resposta é um EventStream: precisamos iterar sobre os eventos
    chunks = []
    for event in response.get("completion", []):
        # Eventos esperados: {"chunk": {"bytes": b"..."}}, ou mensagens de trace/metadata
        if "chunk" in event and "bytes" in event["chunk"]:
            try:
                chunks.append(event["chunk"]["bytes"].decode("utf-8"))
            except Exception:
                # fallback binário -> ignora
                pass
        # Se quiser depurar:
        # elif "trace" in event: ...
        # elif "internalServerException" in event: ...
    full_text = "".join(chunks).strip()
    return full_text

# -----------------------------
# Helper: tentar extrair JSON útil do agente
# -----------------------------
def try_parse_agent_payload(text: str):
    """
    Tenta encontrar um JSON no texto (inclusive dentro de blocos ```json ... ```).
    Espera estrutura: {"sql": "...", "rows": [...], "columns": [...]}
    Retorna dict ou None.
    """
    # 1) se o texto inteiro já é um JSON
    try:
        return json.loads(text)
    except Exception:
        pass

    # 2) procurar bloco ```json ... ```
    if "```" in text:
        parts = text.split("```")
        for i in range(len(parts)):
            candidate = parts[i].strip()
            if candidate.lower().startswith("json"):
                candidate = candidate[4:].strip()
            try:
                return json.loads(candidate)
            except Exception:
                continue

    # 3) heurística: pega o primeiro trecho que parece JSON
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start:end+1]
        try:
            return json.loads(candidate)
        except Exception:
            pass

    return None

# -----------------------------
# UI principal
# -----------------------------
col_input, col_hist = st.columns([3, 2])

with col_input:
    st.subheader("Pergunte sobre os dados (Madrid)")
    pergunta = st.text_input(
        "Ex.: “Quais são os bairros com preço médio mais baixo?”",
        key="user_prompt"
    )
    consultar = st.button("Consultar agente")

with col_hist:
    st.subheader("Histórico")
    if len(st.session_state.history) == 0:
        st.caption("Sem interações ainda.")
    else:
        for msg in reversed(st.session_state.history[-12:]):  # últimas 12
            role = "🧑‍💻 Você" if msg["role"] == "user" else "🤖 Agente"
            st.markdown(f"**{role}:** {msg['content']}")

st.divider()

if consultar and pergunta:
    st.session_state.history.append({"role": "user", "content": pergunta})

    with st.spinner("Consultando Bedrock Agent..."):
        t0 = time.time()
        raw_text = invoke_agent_stream(
            prompt=pergunta,
            agent_id=AGENT_ID,
            alias_id=AGENT_ALIAS_ID,
            region=AWS_REGION,
            session_id=st.session_state.session_id
        )
        elapsed = time.time() - t0

    # Guarda a resposta bruta no histórico (útil para auditoria)
    st.session_state.history.append({"role": "assistant", "content": raw_text})

    # Tenta extrair payload estruturado
    payload = try_parse_agent_payload(raw_text)

    # Layout de resultado
    left, right = st.columns([1.2, 1])

    with left:
        st.subheader("Resposta do agente")
        if payload is None:
            # nada estruturado: mostra texto puro
            st.write(raw_text if raw_text else "_(resposta vazia)_")
        else:
            # Mostra SQL se houver
            sql = payload.get("sql")
            if sql:
                st.markdown("**SQL gerado**")
                st.code(sql, language="sql")

            # Mostra a tabela se vier `rows` e `columns`
            rows = payload.get("rows")
            cols = payload.get("columns")
            if isinstance(rows, list) and isinstance(cols, list) and len(cols) > 0:
                try:
                    df = pd.DataFrame(rows, columns=cols)
                except Exception:
                    # fallback quando linhas já vêm como dicts
                    df = pd.DataFrame(rows)

                st.markdown("**Resultado**")
                st.dataframe(df, use_container_width=True)

                # Download CSV
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📥 Baixar CSV",
                    data=csv,
                    file_name="resultado.csv",
                    mime="text/csv"
                )
            else:
                # Se não tem tabela, exibe a resposta textual/JSON
                st.write(payload)

    with right:
        st.subheader("Metadados")
        st.metric("Tempo de resposta", f"{elapsed:.2f} s")
        st.caption(f"Session ID: `{st.session_state.session_id}`")
        st.caption(f"Agente: {AGENT_ID} / Alias: {AGENT_ALIAS_ID} / Região: {AWS_REGION}")

        with st.expander("Resposta bruta (debug)"):
            st.code(raw_text or "<vazio>")

st.caption("Dica: se o agente às vezes não retornar JSON, ajuste o prompt de sistema do Agent para sempre responder com `{sql, rows, columns}` quando executar Athena.")
