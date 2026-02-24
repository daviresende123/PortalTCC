"""Serviço de embeddings — converte registros JSONB em vetores no ChromaDB."""
import logging
from typing import List, Dict, Any

from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_chroma import Chroma

from config import settings

logger = logging.getLogger(__name__)

_vector_store: Chroma | None = None


def _record_to_text(data: dict, file_name: str = "") -> str:
    """Converte um registro JSONB em texto legível para embedding."""
    pairs = [f"{k}: {v}" for k, v in data.items() if v is not None]
    text = ", ".join(pairs)
    if file_name:
        text = f"arquivo: {file_name} | {text}"
    return text


def get_embeddings_model() -> GoogleGenerativeAIEmbeddings:
    """Retorna instância configurada do modelo de embeddings Google."""
    return GoogleGenerativeAIEmbeddings(
        model=settings.embedding_model,
        google_api_key=settings.google_api_key,
    )


def get_vector_store() -> Chroma:
    """Retorna o vector store ChromaDB (singleton)."""
    global _vector_store
    if _vector_store is None:
        _vector_store = Chroma(
            collection_name=settings.chroma_collection_name,
            embedding_function=get_embeddings_model(),
            persist_directory=settings.chroma_persist_dir,
        )
    return _vector_store


async def embed_records(
    records: List[Dict[str, Any]],
    file_id: int,
    file_name: str,
) -> int:
    """
    Converte registros JSONB em documentos de texto, gera embeddings
    e armazena no ChromaDB.

    Returns:
        Número de documentos embeddados.
    """
    store = get_vector_store()

    texts = []
    metadatas = []
    ids = []

    for i, record_data in enumerate(records):
        text = _record_to_text(record_data, file_name)
        texts.append(text)
        metadatas.append({
            "file_id": file_id,
            "file_name": file_name,
            "record_index": i,
        })
        ids.append(f"file_{file_id}_record_{i}")

    # Processa em lotes de 100 para respeitar limites da API
    BATCH_SIZE = 100
    for start in range(0, len(texts), BATCH_SIZE):
        end = start + BATCH_SIZE
        store.add_texts(
            texts=texts[start:end],
            metadatas=metadatas[start:end],
            ids=ids[start:end],
        )

    logger.info(
        f"Embeddings gerados: {len(texts)} documentos para '{file_name}' (file_id={file_id})"
    )
    return len(texts)
