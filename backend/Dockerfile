# Portal TCC — imagem da aplicação FastAPI.
# O contexto de build é a raiz do repositório (ver docker-compose.yml).
#
# Python 3.12 é obrigatório: asyncpg 0.29.0 não tem wheel para 3.13+ e
# precisaria de compilador na imagem.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Camada própria para as dependências: alterar o código não reinstala tudo.
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ /app/
# O frontend é servido pelo próprio FastAPI (StaticFiles).
COPY frontend/ /app/frontend/

# O diretório do ChromaDB é criado aqui para que o volume nomeado herde a
# propriedade correta na primeira inicialização.
RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /app/chroma_db \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
