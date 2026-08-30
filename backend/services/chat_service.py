"""
Serviço de chat — LangChain + Gemini com function calling.

O modelo recebe o *esquema* do dataset e a descrição das ferramentas
(services/tools.py), e decide sozinho quais chamar. O PostgreSQL calcula os
números, o ChromaDB busca por semelhança, e o modelo interpreta os resultados.
"""
import json
import logging
import time
from typing import Any, AsyncGenerator, Dict, List

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from config import settings
from services import tools
from services.query_service import descrever_dataset

logger = logging.getLogger(__name__)

# session_id -> [(pergunta, resposta)]. Guarda só o texto final de cada troca:
# resultados de ferramenta são grandes e só interessam dentro da própria rodada.
_sessions: Dict[str, List[tuple]] = {}

MAX_TROCAS_HISTORICO = 10

SYSTEM_PROMPT = """Você é um assistente de análise de dados do Portal TCC.
Responda sempre em português brasileiro.

Você NÃO tem os dados em mãos. Para obter qualquer informação sobre eles, use
as ferramentas disponíveis — elas consultam o banco de dados real.

Regras que você deve seguir sempre:

1. NUNCA invente, estime ou calcule números por conta própria. Toda média,
   mediana, contagem, soma ou comparação deve vir de uma ferramenta. Se você
   se pegar somando valores mentalmente, pare e chame `estatisticas`.
2. Se a pergunta pede estatística de vários ou de todos os elementos, chame
   `estatisticas` UMA vez sem o argumento `colunas` — ela devolve todas as
   colunas numéricas de uma vez. Não faça uma chamada por coluna.
3. Apresente resultados completos. Se a ferramenta devolveu 57 colunas, mostre
   as 57 — não resuma, não trunque, não escreva "entre outros". Use tabelas
   em Markdown quando houver muitos números.
4. Se uma ferramenta devolver um campo "erro", leia a mensagem, corrija os
   argumentos e tente de novo.
5. Se os dados realmente não contiverem a informação, diga isso claramente,
   mencionando o que você consultou.

Estrutura dos dados carregados:

{esquema}"""

SEM_DADOS = (
    "Ainda não há dados carregados no sistema. "
    "Envie um arquivo CSV na página inicial para poder fazer perguntas sobre ele."
)

SEM_RESPOSTA = (
    "Consultei os dados, mas não consegui montar uma resposta. "
    "Tente reformular a pergunta de forma mais específica."
)


def _get_llm() -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model=settings.llm_model,
        google_api_key=settings.google_api_key,
        temperature=settings.llm_temperature,
        timeout=settings.llm_timeout_seconds,
        max_retries=settings.llm_max_retries,
    )


def _get_chat_history(session_id: str) -> List[tuple]:
    if session_id not in _sessions:
        _sessions[session_id] = []
    return _sessions[session_id]


def _resumo_esquema(descricao: dict) -> str:
    """
    Condensa descrever_dataset() para o prompt do sistema.

    Evita uma rodada de ferramenta só para o modelo descobrir os nomes das
    colunas. Apenas a estrutura entra aqui, nunca os dados.
    """
    linhas = []

    arquivos = descricao.get("arquivos", [])
    if arquivos:
        nomes = ", ".join(f"{a['nome']} ({a['registros']} registros)" for a in arquivos)
        linhas.append(f"Arquivos: {nomes}")
    linhas.append(f"Total de registros: {descricao.get('total_registros', 0)}")

    colunas = descricao.get("colunas", [])
    numericas = [c["nome"] for c in colunas if c["tipo"] == "numérica"]
    texto = [c for c in colunas if c["tipo"] == "texto"]

    if numericas:
        linhas.append(
            f"\nColunas numéricas ({len(numericas)}): {', '.join(numericas)}"
        )
    if texto:
        linhas.append(f"\nColunas de texto ({len(texto)}):")
        for c in texto:
            distintos = c.get("distintos")
            valores = c.get("valores")
            if valores:
                linhas.append(
                    f"  - {c['nome']} ({distintos} distintos): {', '.join(valores)}"
                )
            else:
                linhas.append(f"  - {c['nome']} ({distintos} valores distintos)")

    return "\n".join(linhas)


def _texto_do_chunk(conteudo: Any) -> str:
    """Normaliza o content de um chunk — o Gemini às vezes devolve lista."""
    if isinstance(conteudo, str):
        return conteudo
    if isinstance(conteudo, list):
        return "".join(
            parte if isinstance(parte, str) else parte.get("text", "")
            for parte in conteudo
        )
    return ""


async def _montar_mensagens(question: str, session_id: str) -> list | None:
    """Monta a conversa inicial. Retorna None quando não há dados carregados."""
    descricao = await descrever_dataset()
    if not descricao.get("total_registros"):
        return None

    mensagens = [
        SystemMessage(content=SYSTEM_PROMPT.format(esquema=_resumo_esquema(descricao)))
    ]
    for pergunta, resposta in _get_chat_history(session_id)[-MAX_TROCAS_HISTORICO:]:
        mensagens.append(HumanMessage(content=pergunta))
        mensagens.append(AIMessage(content=resposta))
    mensagens.append(HumanMessage(content=question))
    return mensagens


async def chat_stream(
    question: str,
    session_id: str = "default",
) -> AsyncGenerator[dict, None]:
    """
    Processa uma pergunta e emite eventos {"tipo": "status"|"token", "texto"}.

    Os eventos de status informam qual ferramenta está rodando e mantêm bytes
    trafegando no SSE enquanto nenhum token de resposta é gerado.
    """
    t_inicio = time.perf_counter()
    t_primeiro_token = None
    chamadas = 0

    mensagens = await _montar_mensagens(question, session_id)
    if mensagens is None:
        yield {"tipo": "token", "texto": SEM_DADOS}
        return

    llm = _get_llm().bind_tools(tools.ESQUEMAS)
    resposta_final = ""

    for iteracao in range(settings.chat_max_iteracoes):
        acumulado = None
        async for chunk in llm.astream(mensagens):
            acumulado = chunk if acumulado is None else acumulado + chunk
            trecho = _texto_do_chunk(chunk.content)
            if trecho:
                if t_primeiro_token is None:
                    t_primeiro_token = time.perf_counter()
                resposta_final += trecho
                yield {"tipo": "token", "texto": trecho}

        if acumulado is None:
            break

        mensagens.append(acumulado)
        tool_calls = getattr(acumulado, "tool_calls", None) or []

        if not tool_calls:
            break

        # Na última iteração não adianta executar ferramentas cujo resultado
        # o modelo não terá chance de ler.
        if iteracao == settings.chat_max_iteracoes - 1:
            logger.warning(
                f"Limite de {settings.chat_max_iteracoes} rodadas de ferramenta "
                f"atingido para: {question[:80]!r}"
            )
            mensagens.pop()
            break

        for chamada in tool_calls:
            chamadas += 1
            nome, argumentos = chamada["name"], chamada.get("args") or {}
            yield {"tipo": "status", "texto": tools.rotulo(nome, argumentos)}
            logger.info(f"Ferramenta: {nome}({json.dumps(argumentos, ensure_ascii=False)})")
            resultado = await tools.executar(nome, argumentos)
            mensagens.append(
                ToolMessage(content=resultado, tool_call_id=chamada["id"])
            )

    if resposta_final.strip():
        historico = _get_chat_history(session_id)
        historico.append((question, resposta_final))
        del historico[:-MAX_TROCAS_HISTORICO]
    else:
        # O modelo gastou todas as rodadas em ferramentas sem redigir nada;
        # sem isto o usuário receberia um balão vazio. Não entra no histórico.
        logger.warning(f"Nenhum texto gerado para: {question[:80]!r}")
        yield {"tipo": "token", "texto": SEM_RESPOSTA}

    ttft = (t_primeiro_token - t_inicio) if t_primeiro_token else 0.0
    logger.info(
        f"[METRICS] chat_stream: TTFT={ttft:.3f}s "
        f"tempo_total={time.perf_counter() - t_inicio:.3f}s ferramentas={chamadas}"
    )


async def chat(question: str, session_id: str = "default") -> str:
    """Versão sem streaming: consome chat_stream e devolve só o texto final."""
    partes = [
        evento["texto"]
        async for evento in chat_stream(question, session_id)
        if evento["tipo"] == "token"
    ]
    return "".join(partes)


def clear_session(session_id: str) -> None:
    """Limpa o histórico de conversa de uma sessão."""
    _sessions.pop(session_id, None)
