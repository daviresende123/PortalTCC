"""Serviço de chat RAG — LangChain + Gemini + ChromaDB."""
import asyncio
import csv
import io
import re
import logging
import time
from typing import AsyncGenerator, Dict, List

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from config import settings
from services.embedding_service import get_vector_store
from services.db_service import (
    get_all_records,
    get_column_stats,
    get_dataset_overview,
)

logger = logging.getLogger(__name__)

# Memória de sessão em memória: session_id -> [(pergunta, resposta)]
_sessions: Dict[str, List[tuple]] = {}

# Quantos registros a busca vetorial devolve para perguntas pontuais.
# Só vale para consultas de registro único: agregações recebem o dataset
# inteiro, lido do PostgreSQL (ver _retrieve_context).
DEFAULT_K = 10

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
        # Sem timeout uma chamada sob rate limit fica em retry indefinido e o
        # chat trava carregando para sempre; melhor falhar de forma visível.
        timeout=settings.llm_timeout_seconds,
        max_retries=settings.llm_max_retries,
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


# Nome do elemento em português -> símbolo usado como nome de coluna nos
# arquivos pXRF. Permite que "média de zinco" encontre a coluna "Zn".
_ELEMENTOS_PT = {
    "magnesio": "Mg", "magnésio": "Mg",
    "aluminio": "Al", "alumínio": "Al",
    "silicio": "Si", "silício": "Si",
    "fosforo": "P", "fósforo": "P",
    "enxofre": "S",
    "cloro": "Cl",
    "potassio": "K", "potássio": "K",
    "calcio": "Ca", "cálcio": "Ca",
    "titanio": "Ti", "titânio": "Ti",
    "vanadio": "V", "vanádio": "V",
    "cromo": "Cr", "cromio": "Cr", "crômio": "Cr",
    "manganes": "Mn", "manganês": "Mn",
    "ferro": "Fe",
    "niquel": "Ni", "níquel": "Ni",
    "cobre": "Cu",
    "zinco": "Zn",
    "arsenio": "As", "arsênio": "As", "arsenico": "As", "arsênico": "As",
    "selenio": "Se", "selênio": "Se",
    "bromo": "Br",
    "rubidio": "Rb", "rubídio": "Rb",
    "estroncio": "Sr", "estrôncio": "Sr",
    "molibdenio": "Mo", "molibdênio": "Mo",
    "cadmio": "Cd", "cádmio": "Cd",
    "bario": "Ba", "bário": "Ba",
    "mercurio": "Hg", "mercúrio": "Hg",
    "chumbo": "Pb",
}


def _resolve_numeric_column(question: str, all_columns: List[str]) -> str | None:
    """
    Descobre a qual coluna a pergunta se refere ("zinco" -> "Zn").

    Retorna None quando não dá para decidir — nesse caso simplesmente não
    injetamos estatística, e o modelo trabalha com a tabela completa.
    """
    if not all_columns:
        return None

    q_lower = question.lower()
    tokens = re.findall(r"[0-9A-Za-zÀ-ÿ#]+", question)
    lower_tokens = {t.lower() for t in tokens}

    # As colunas "* Err" são a incerteza da medição, não a medição. Só entram
    # se o usuário falar de erro explicitamente — do contrário "média de Zn"
    # poderia cair em "Zn Err" e devolver um número sem sentido.
    wants_err = bool(re.search(r"\berr\w*|incerteza", q_lower))
    candidates = [
        c for c in all_columns
        if wants_err or not re.search(r"\berr\b", c.lower())
    ]
    by_lower = {c.lower(): c for c in candidates}

    def pick(col: str) -> str:
        """Se o usuário falou de erro, prefere a coluna irmã '<col> Err'."""
        if wants_err:
            sibling = by_lower.get(f"{col.lower()} err")
            if sibling:
                return sibling
        return col

    # 1) nome do elemento em português
    for token in lower_tokens:
        symbol = _ELEMENTOS_PT.get(token)
        if symbol and symbol.lower() in by_lower:
            return pick(by_lower[symbol.lower()])

    # 2) nome da coluna citado literalmente (2+ caracteres)
    for col in sorted(candidates, key=len, reverse=True):
        cl = col.lower()
        if len(cl) >= 2 and re.search(
            rf"(?<![0-9a-zà-ÿ]){re.escape(cl)}(?![0-9a-zà-ÿ])", q_lower
        ):
            return pick(col)

    # 3) símbolo de uma letra só (P, S, K, V...) exige grafia exata, senão
    #    qualquer "a" ou "e" solto na frase viraria nome de coluna
    for col in candidates:
        if len(col) == 1 and col in tokens:
            return pick(col)

    return None


def _format_stats_as_context(stats: dict) -> str:
    """Formata o resultado de get_column_stats para o prompt."""
    def fmt(value):
        return "—" if value is None else f"{value:.6g}"

    return (
        f"=== Estatísticas da coluna '{stats['column']}' calculadas pelo banco de dados ===\n"
        f"Valores exatos, computados em SQL sobre {stats['count']} de "
        f"{stats['total_records']} registros:\n"
        f"  média: {fmt(stats['media'])}\n"
        f"  mínimo: {fmt(stats['minimo'])}\n"
        f"  máximo: {fmt(stats['maximo'])}\n"
        f"  desvio padrão (amostral): {fmt(stats['desvio'])}\n"
        f"  soma: {fmt(stats['soma'])}"
    )


def _format_records_as_table(full: dict) -> str:
    """
    Converte todos os registros em uma tabela CSV para o contexto.

    Formato de tabela em vez de "coluna: valor" repetido linha a linha: os
    nomes das colunas aparecem UMA vez, no cabeçalho, em vez de uma vez por
    registro. Para o pXRF de exemplo isso é ~4.800 tokens em vez de ~10.900,
    com exatamente a mesma informação.

    Nada aqui altera o arquivo original nem o banco: os dados já chegam
    colunados, do JSONB gravado no upload.
    """
    records = full["records"]
    columns = full["columns"] or sorted({k for r in records for k in r["data"]})
    multi_file = len({r["file_name"] for r in records}) > 1

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow((["arquivo"] if multi_file else []) + columns)
    for rec in records:
        data = rec["data"]
        values = ["" if data.get(c) is None else data.get(c) for c in columns]
        writer.writerow(([rec["file_name"]] if multi_file else []) + values)

    n = len(records)
    header = (
        f"=== TODOS OS {n} REGISTROS DO DATASET, EM CSV "
        f"({n} de {full['total_records']} — dataset completo, nada foi omitido) ==="
    )
    return header + "\n" + buf.getvalue().rstrip("\n")


async def _vector_context(question: str) -> str:
    """
    Busca os DEFAULT_K registros mais parecidos com a pergunta no ChromaDB.

    O asyncio.wait_for é o que impede o carregamento infinito: a busca embeda
    a pergunta pela API do Google, e essa chamada não tem timeout próprio —
    sob rate limit (429) o cliente fica em retry sem nunca desistir.
    """
    store = get_vector_store()
    retriever = store.as_retriever(
        search_type="similarity",
        search_kwargs={"k": DEFAULT_K},
    )
    try:
        docs = await asyncio.wait_for(
            retriever.ainvoke(question),
            timeout=settings.retrieval_timeout_seconds,
        )
    except asyncio.TimeoutError:
        logger.error(
            f"Busca vetorial excedeu {settings.retrieval_timeout_seconds}s "
            f"(possível rate limit da API de embeddings)"
        )
        return ""
    except Exception as exc:
        logger.error(f"Falha na busca vetorial: {exc}")
        return ""

    return "\n\n".join(doc.page_content for doc in docs)


async def _retrieve_context(question: str) -> tuple[str, bool]:
    """
    Monta o contexto da pergunta.

    Consultas agregadas (média, contagem, listagem) recebem o dataset INTEIRO
    lido do PostgreSQL — enquanto ele couber no contexto do modelo. Isso
    elimina de vez o "só tenho 10 dos 37 registros" e ainda dispensa a chamada
    de embedding, que é uma ida à API do Google a menos por pergunta.

    Consultas pontuais ("qual o Zn da amostra X") continuam na busca vetorial,
    que é a ferramenta certa para esse caso.

    Returns (context_text, is_aggregation).
    """
    is_agg = _is_aggregation_query(question)

    if not is_agg:
        context = await _vector_context(question)
        return (context or "Nenhum dado encontrado no banco de dados."), False

    parts: List[str] = []
    all_columns: List[str] = []

    try:
        overview = await get_dataset_overview()
        parts.append(
            "=== Resumo do dataset ===\n" + _format_overview_as_context(overview)
        )
        all_columns = overview.get("all_columns", [])
    except Exception as exc:
        logger.warning(f"Falha ao obter visão geral do dataset: {exc}")

    # Estatística exata em SQL: o modelo é ruim em somar 37 floats na mão,
    # então quando dá para identificar a coluna entregamos o número pronto.
    column = _resolve_numeric_column(question, all_columns)
    if column:
        try:
            stats = await get_column_stats(column)
            if stats:
                parts.append(_format_stats_as_context(stats))
        except Exception as exc:
            logger.warning(f"Falha ao calcular estatísticas de '{column}': {exc}")

    full = None
    try:
        full = await get_all_records()
    except Exception as exc:
        logger.warning(f"Falha ao carregar todos os registros: {exc}")

    if full and not full["truncated"] and full["records"]:
        parts.append(_format_records_as_table(full))
    else:
        # Plano B: dataset grande demais (ou banco indisponível). Aqui a
        # amostra é rotulada como amostra, para o modelo não confundi-la
        # com o dataset completo.
        vector_context = await _vector_context(question)
        if vector_context:
            total = full["total_records"] if full else "?"
            parts.append(
                f"=== AMOSTRA ILUSTRATIVA: {DEFAULT_K} registros de {total} "
                f"(NÃO use para contagens ou cálculos) ===\n" + vector_context
            )

    if not parts:
        return "Nenhum dado encontrado no banco de dados.", True

    return "\n\n".join(parts), True


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
    _t_start = time.perf_counter()  # [METRICS]
    _t_ttft = None  # [METRICS]

    context, is_agg = await _retrieve_context(question)
    history = _get_chat_history(session_id)
    messages = _build_messages(context, history, question, aggregation=is_agg)

    llm = _get_llm()
    full_response = ""
    async for chunk in llm.astream(messages):
        token = chunk.content
        if token:
            if _t_ttft is None:  # [METRICS] primeiro token
                _t_ttft = time.perf_counter()
            full_response += token
            yield token

    _t_end = time.perf_counter()  # [METRICS]
    ttft = (_t_ttft - _t_start) if _t_ttft else 0.0  # [METRICS]
    total = _t_end - _t_start  # [METRICS]
    logger.info(  # [METRICS]
        f"[METRICS] chat_stream: TTFT={ttft:.3f}s tempo_total={total:.3f}s "
        f"is_agg={is_agg}"
    )

    history.append((question, full_response))


def clear_session(session_id: str) -> None:
    """Limpa o histórico de conversa de uma sessão."""
    _sessions.pop(session_id, None)
