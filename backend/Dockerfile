# Portal TCC — imagem da aplicação FastAPI
#
# Python 3.12 é obrigatório: asyncpg 0.29.0 não possui wheel para 3.13+
# e falharia ao compilar. Com 3.12 todas as dependências instalam a
# partir de wheels prontas, sem necessidade de compilador na imagem.
#
# O contexto de build é a RAIZ do repositório (ver docker-compose.yml),
# porque a imagem precisa do backend e do frontend.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependências em camada própria: alterar código-fonte não reinstala tudo
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código da aplicação
COPY backend/ /app/
# O frontend é servido pelo próprio FastAPI (StaticFiles), então acompanha a imagem
COPY frontend/ /app/frontend/

# Usuário não-root. O diretório do ChromaDB é criado aqui para que o
# volume nomeado herde a propriedade correta na primeira inicialização.
RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /app/chroma_db \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
