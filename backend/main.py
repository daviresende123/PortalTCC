"""Aplicação principal FastAPI."""
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from db.connection import init_db
from routes import upload, chat
from config import settings
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicializa o banco de dados na subida da aplicação."""
    logger.info("Inicializando banco de dados...")
    await init_db()
    logger.info("Banco de dados pronto")
    yield


app = FastAPI(
    title="Portal TCC - API",
    description="API para upload de dados CSV para PostgreSQL",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url, "http://localhost:5500", "http://127.0.0.1:5500"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload.router)
app.include_router(chat.router)


@app.get("/api/info")
async def info():
    """Informações da API."""
    return {
        "message": "Portal TCC API",
        "status": "online",
        "version": "2.0.0"
    }


@app.get("/health")
async def health():
    """Health check."""
    return {"status": "healthy"}


# Frontend estático. Este mount precisa ser o ÚLTIMO registro de rota: o
# Starlette resolve as rotas na ordem de registro e "/" captura tudo.
_BASE_DIR = Path(__file__).resolve().parent
# Docker copia o frontend para backend/frontend; localmente ele fica na raiz.
FRONTEND_DIR = _BASE_DIR / "frontend"
if not FRONTEND_DIR.is_dir():
    FRONTEND_DIR = _BASE_DIR.parent / "frontend"

if FRONTEND_DIR.is_dir():
    app.mount(
        "/",
        StaticFiles(directory=FRONTEND_DIR, html=True),
        name="frontend",
    )
    logger.info(f"Frontend servido a partir de {FRONTEND_DIR}")
else:
    logger.warning(
        f"Diretório do frontend não encontrado (procurado em "
        f"{_BASE_DIR / 'frontend'} e {_BASE_DIR.parent / 'frontend'}); "
        f"apenas a API está disponível"
    )


if __name__ == "__main__":
    import uvicorn
    logger.info(f"Iniciando servidor em {settings.host}:{settings.port}")
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=True
    )
