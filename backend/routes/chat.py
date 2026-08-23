"""Rotas do chatbot RAG."""
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

# Resposta usada quando não há chave da API configurada. Permite que a
# aplicação suba e que o fluxo de upload seja demonstrado sem credenciais,
# em vez de estourar uma exceção que o frontend exibiria como falha de
# conexão com o servidor. Renderizada como Markdown no balão do chat.
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
        logger.error(f"Erro no chat: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao processar mensagem: {str(e)}",
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
            async for token in chat_stream(request.message, session_id):
                data = json.dumps({"token": token}, ensure_ascii=False)
                yield f"data: {data}\n\n"
            yield f"event: done\ndata: {json.dumps({'session_id': session_id})}\n\n"
        except Exception as e:
            logger.error(f"Erro no stream: {e}")
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@router.delete("/session/{session_id}")
async def clear_session_endpoint(session_id: str):
    """Limpa o histórico de conversa de uma sessão."""
    clear_session(session_id)
    return {"message": "Sessão limpa com sucesso"}
