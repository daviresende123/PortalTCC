"""Rotas do chatbot."""
import asyncio
import json
import uuid
import logging
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from config import settings
from services.chat_service import chat, chat_stream, clear_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chat", tags=["chat"])

# Sem chave configurada o chat responde isto (em Markdown) em vez de estourar
# uma exceção, para que o fluxo de upload continue utilizável sem credenciais.
NO_API_KEY_MESSAGE = (
    "⚠️ **Chave da API do Google não configurada.**\n\n"
    "O chat precisa de uma chave do Gemini para responder. "
    "O upload de arquivos CSV funciona normalmente sem ela.\n\n"
    "Para ativar o chat:\n\n"
    "1. Gere uma chave gratuita em "
    "[aistudio.google.com/apikey](https://aistudio.google.com/apikey)\n"
    "2. Cole no arquivo `.env`: `GOOGLE_API_KEY=sua-chave-aqui`\n"
    "3. Reinicie a aplicação: `docker compose restart backend`"
)


async def _com_heartbeat(gerador, intervalo: float):
    """
    Repassa os eventos do gerador e emite ("heartbeat", None) sempre que ele
    passar `intervalo` segundos calado.

    Enquanto o Gemini raciocina antes do primeiro token nenhum byte trafega, e
    o timer de ociosidade do navegador abortaria a requisição.
    """
    fila: asyncio.Queue = asyncio.Queue()

    async def produzir():
        try:
            async for evento in gerador:
                await fila.put(("evento", evento))
            await fila.put(("fim", None))
        except Exception as exc:
            await fila.put(("erro", exc))

    tarefa = asyncio.create_task(produzir())
    try:
        while True:
            try:
                tipo, valor = await asyncio.wait_for(fila.get(), timeout=intervalo)
            except asyncio.TimeoutError:
                yield ("heartbeat", None)
                continue
            if tipo == "fim":
                return
            if tipo == "erro":
                raise valor
            yield ("evento", valor)
    finally:
        tarefa.cancel()


def _mensagem_amigavel(exc: Exception) -> str:
    """
    Traduz falhas conhecidas da API em algo acionável.

    O erro cru do Google traz dezenas de linhas de `quota_dimensions` que não
    dizem ao usuário o que fazer.
    """
    texto = str(exc)
    if "429" in texto or "quota" in texto.lower():
        return (
            "Limite de uso da API do Google atingido. O plano gratuito do "
            "Gemini permite um número limitado de perguntas por dia. "
            "Aguarde alguns minutos (ou até amanhã, se for o limite diário) "
            "e tente novamente."
        )
    if "API key" in texto or "API_KEY" in texto or "401" in texto:
        return (
            "A chave da API do Google foi recusada. Verifique o valor de "
            "GOOGLE_API_KEY no arquivo .env e reinicie o backend."
        )
    if "timeout" in texto.lower() or "deadline" in texto.lower():
        return (
            "A API do Google demorou demais para responder. "
            "Tente novamente em alguns instantes."
        )
    return f"Erro ao processar a pergunta: {texto}"


class ChatRequest(BaseModel):
    """Corpo da requisição de chat."""
    message: str
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    """Corpo da resposta de chat."""
    answer: str
    session_id: str


@router.post("", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    """Envia uma mensagem e recebe resposta completa."""
    session_id = request.session_id or str(uuid.uuid4())

    if not settings.google_api_key:
        logger.warning("Chat acionado sem GOOGLE_API_KEY configurada")
        return ChatResponse(answer=NO_API_KEY_MESSAGE, session_id=session_id)

    try:
        answer = await chat(request.message, session_id)
        return ChatResponse(answer=answer, session_id=session_id)
    except Exception as e:
        logger.error(f"Erro no chat: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_mensagem_amigavel(e),
        )


@router.post("/stream")
async def chat_stream_endpoint(request: ChatRequest):
    """Envia uma mensagem e recebe resposta via SSE streaming."""
    session_id = request.session_id or str(uuid.uuid4())

    async def event_generator():
        if not settings.google_api_key:
            logger.warning("Chat acionado sem GOOGLE_API_KEY configurada")
            data = json.dumps({"token": NO_API_KEY_MESSAGE}, ensure_ascii=False)
            yield f"data: {data}\n\n"
            yield f"event: done\ndata: {json.dumps({'session_id': session_id})}\n\n"
            return

        try:
            eventos = _com_heartbeat(
                chat_stream(request.message, session_id),
                settings.sse_heartbeat_seconds,
            )
            async for tipo, evento in eventos:
                if tipo == "heartbeat":
                    # Comentário SSE: o cliente ignora, mas é um byte na rede.
                    yield ": keep-alive\n\n"
                    continue
                chave = "status" if evento["tipo"] == "status" else "token"
                data = json.dumps({chave: evento["texto"]}, ensure_ascii=False)
                yield f"data: {data}\n\n"
            yield f"event: done\ndata: {json.dumps({'session_id': session_id})}\n\n"
        except Exception as e:
            logger.error(f"Erro no stream: {e}", exc_info=True)
            payload = json.dumps({"error": _mensagem_amigavel(e)}, ensure_ascii=False)
            yield f"event: error\ndata: {payload}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # Sem isto um proxy bufferiza o SSE e os eventos de status só
            # chegam junto com a resposta.
            "X-Accel-Buffering": "no",
        },
    )


@router.delete("/session/{session_id}")
async def clear_session_endpoint(session_id: str):
    """Limpa o histórico de conversa de uma sessão."""
    clear_session(session_id)
    return {"message": "Sessão limpa com sucesso"}
