"""Rotas do chatbot RAG."""
import json
import uuid
import logging
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from services.chat_service import chat, chat_stream, clear_session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chat", tags=["chat"])


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
    try:
        session_id = request.session_id or str(uuid.uuid4())
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
