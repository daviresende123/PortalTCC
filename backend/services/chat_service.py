"""Serviço de chat RAG — LangChain + Gemini + ChromaDB."""
import logging
from typing import AsyncGenerator, Dict, List

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from config import settings
from services.embedding_service import get_vector_store

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


def _build_messages(context: str, history: List[tuple], question: str) -> list:
    """Monta a lista de mensagens LangChain com contexto, histórico e pergunta."""
    messages = [SystemMessage(content=SYSTEM_PROMPT.format(context=context))]
    for human_msg, ai_msg in history[-10:]:
        messages.append(HumanMessage(content=human_msg))
        messages.append(AIMessage(content=ai_msg))
    messages.append(HumanMessage(content=question))
    return messages


async def chat(question: str, session_id: str = "default") -> str:
    """
    Processa uma pergunta pelo pipeline RAG.

    1. Recupera documentos relevantes do ChromaDB
    2. Constrói prompt com contexto + histórico
    3. Envia para Gemini 2.0 Flash
    4. Armazena troca na memória da sessão
    """
    store = get_vector_store()
    retriever = store.as_retriever(
        search_type="similarity",
        search_kwargs={"k": 5},
    )

    docs = await retriever.ainvoke(question)
    context = "\n\n".join(doc.page_content for doc in docs)

    if not context.strip():
        context = "Nenhum dado encontrado no banco de dados."

    history = _get_chat_history(session_id)
    messages = _build_messages(context, history, question)

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
    store = get_vector_store()
    retriever = store.as_retriever(
        search_type="similarity",
        search_kwargs={"k": 5},
    )

    docs = await retriever.ainvoke(question)
    context = "\n\n".join(doc.page_content for doc in docs)

    if not context.strip():
        context = "Nenhum dado encontrado no banco de dados."

    history = _get_chat_history(session_id)
    messages = _build_messages(context, history, question)

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
