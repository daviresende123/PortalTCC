"""Serviço de chat RAG — LangChain + Gemini + ChromaDB."""
import re
import logging
from typing import AsyncGenerator, Dict, List

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from config import settings
from services.embedding_service import get_vector_store
from services.db_service import get_dataset_overview

logger = logging.getLogger(__name__)

# Memória de sessão em memória: session_id -> [(pergunta, resposta)]
_sessions: Dict[str, List[tuple]] = {}

SYSTEM_PROMPT = """Você é um assistente especializado em analisar dados do Portal TCC.
Você tem acesso a dados de arquivos CSV que foram carregados no sistema.
Use APENAS os dados fornecidos no contexto abaixo para responder as perguntas do usuário.
Se você não encontrar a informação nos dados, diga claramente que não encontrou.
Responda sempre em português brasileiro.
Seja conciso e direto nas respostas.

Contexto dos dados:
{context}"""

# Used for dataset-wide queries: instructs the model not to summarize or truncate.
AGGREGATION_SYSTEM_PROMPT = """Você é um assistente especializado em analisar dados do Portal TCC.
Você tem acesso a dados de arquivos CSV que foram carregados no sistema.
Use APENAS os dados fornecidos no contexto abaixo para responder as perguntas do usuário.
Se você não encontrar a informação nos dados, diga claramente que não encontrou.
Responda sempre em português brasileiro.

IMPORTANTE: quando o usuário pede uma lista completa, enumeração, contagem ou agregação,
você DEVE apresentar TODOS os valores fornecidos no contexto — não resuma, não trunce,
não use "etc.", não diga "entre outros". Liste cada item individualmente.

Contexto dos dados:
{context}"""

# Patterns that indicate the user wants aggregate or full-dataset information.
_AGGREGATION_PATTERNS = re.compile(
    r"""
    quais\s+(amostras?|arquivos?|elementos?|valores?|registros?|dados?|nomes?) |
    list[ae]                                   |
    listagem                                   |
    todos\s+os?\s+\w+                          |
    todas\s+as?\s+\w+                          |
    quantas?\s+(amostras?|registros?|arquivos?|dados?|elementos?) |
    total\s+de\s+\w+                           |
    distin[ct][oa]s?                           |
    [uú]nico\w*                                |
    presentes?\s+(no|na|nos|nas|em)            |
    quant[oa]s?\s+\w+\s+(h[aá]|exist[ei]\w*)  |
    n[uú]mero\s+de\s+\w+                       |
    contar\s+\w+                               |
    contagem                                   |
    m[eé]dia\s+(de|do|da)                      |
    m[aá]ximo\s+(de|do|da)                     |
    m[ií]nimo\s+(de|do|da)                     |
    desvio\s+padr[aã]o                         |
    ranking                                    |
    list\s+all                                 |
    how\s+many                                 |
    all\s+samples?                             |
    \ball\b.{0,20}\bdata\b                     |
    count\s+of                                 |
    unique\s+\w+
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _is_aggregation_query(question: str) -> bool:
    """Return True when the question asks for aggregate/enumeration information."""
    return bool(_AGGREGATION_PATTERNS.search(question))


def _format_overview_as_context(overview: dict) -> str:
    """Convert a dataset overview dict into a readable context string."""
    lines = []

    files = overview.get("files", [])
    lines.append(f"Arquivos carregados: {len(files)}")
    for f in files:
        lines.append(f"  - {f['file_name']}: {f['rows_count']} registros, colunas: {', '.join(f['columns'])}")

    lines.append(f"\nTotal de registros: {overview['total_records']}")

    samples = overview.get("samples", [])
    if samples:
        lines.append(f"\nAmostras presentes nos dados ({len(samples)} no total):")
        for s in samples:
            lines.append(f"  - {s}")
    else:
        lines.append("\nNenhuma coluna 'amostra' encontrada nos dados.")

    cols = overview.get("all_columns", [])
    if cols:
        lines.append(f"\nColunas disponíveis nos dados: {', '.join(cols)}")

    return "\n".join(lines)


def _get_llm() -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model=settings.llm_model,
        google_api_key=settings.google_api_key,
        temperature=settings.llm_temperature,
        convert_system_message_to_human=True,
    )


def _get_chat_history(session_id: str) -> List[tuple]:
    if session_id not in _sessions:
        _sessions[session_id] = []
    return _sessions[session_id]


def _build_messages(context: str, history: List[tuple], question: str, aggregation: bool = False) -> list:
    """Monta a lista de mensagens LangChain com contexto, histórico e pergunta."""
    prompt_template = AGGREGATION_SYSTEM_PROMPT if aggregation else SYSTEM_PROMPT
    messages = [SystemMessage(content=prompt_template.format(context=context))]
    for human_msg, ai_msg in history[-10:]:
        messages.append(HumanMessage(content=human_msg))
        messages.append(AIMessage(content=ai_msg))
    messages.append(HumanMessage(content=question))
    return messages


async def _retrieve_context(question: str) -> tuple[str, bool]:
    """
    Retrieve the most appropriate context for a question.

    For aggregation/enumeration queries, fetches a full dataset summary from
    PostgreSQL so that every sample, count, or column is accounted for.

    For record-level queries, uses ChromaDB similarity search (k=10).

    Returns (context_text, is_aggregation).
    """
    is_agg = _is_aggregation_query(question)

    if is_agg:
        try:
            overview = await get_dataset_overview()
            db_context = _format_overview_as_context(overview)
        except Exception as exc:
            logger.warning(f"Falha ao obter visão geral do dataset: {exc}")
            db_context = ""

        # Also fetch a few vector-search results for additional record-level detail.
        store = get_vector_store()
        retriever = store.as_retriever(
            search_type="similarity",
            search_kwargs={"k": 10},
        )
        docs = await retriever.ainvoke(question)
        vector_context = "\n\n".join(doc.page_content for doc in docs)

        parts = []
        if db_context:
            parts.append("=== Resumo completo do dataset ===\n" + db_context)
        if vector_context:
            parts.append("=== Registros representativos ===\n" + vector_context)

        context = "\n\n".join(parts) if parts else "Nenhum dado encontrado no banco de dados."
    else:
        store = get_vector_store()
        retriever = store.as_retriever(
            search_type="similarity",
            search_kwargs={"k": 10},
        )
        docs = await retriever.ainvoke(question)
        context = "\n\n".join(doc.page_content for doc in docs)

        if not context.strip():
            context = "Nenhum dado encontrado no banco de dados."

    return context, is_agg


async def chat(question: str, session_id: str = "default") -> str:
    """
    Processa uma pergunta pelo pipeline RAG.

    1. Detecta se é consulta agregada ou de registro único
    2. Recupera documentos relevantes (ChromaDB) e/ou resumo completo (PostgreSQL)
    3. Constrói prompt com contexto + histórico
    4. Envia para Gemini
    5. Armazena troca na memória da sessão
    """
    context, is_agg = await _retrieve_context(question)
    history = _get_chat_history(session_id)
    messages = _build_messages(context, history, question, aggregation=is_agg)

    llm = _get_llm()
    response = await llm.ainvoke(messages)
    answer = response.content

    history.append((question, answer))
    return answer


async def chat_stream(
    question: str,
    session_id: str = "default",
) -> AsyncGenerator[str, None]:
    """Mesmo que chat() mas retorna tokens via streaming."""
    context, is_agg = await _retrieve_context(question)
    history = _get_chat_history(session_id)
    messages = _build_messages(context, history, question, aggregation=is_agg)

    llm = _get_llm()
    full_response = ""
    async for chunk in llm.astream(messages):
        token = chunk.content
        if token:
            full_response += token
            yield token

    history.append((question, full_response))


def clear_session(session_id: str) -> None:
    """Limpa o histórico de conversa de uma sessão."""
    _sessions.pop(session_id, None)
