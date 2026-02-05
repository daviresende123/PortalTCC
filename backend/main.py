"""Aplicação principal FastAPI."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import upload
from config import settings
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Criar aplicação FastAPI
app = FastAPI(
    title="Portal TCC - API",
    description="API para upload de dados CSV para Delta Lake",
    version="1.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url, "http://localhost:5500", "http://127.0.0.1:5500"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar rotas
app.include_router(upload.router)


@app.get("/")
async def root():
    """Endpoint raiz."""
    return {
        "message": "Portal TCC API",
        "status": "online",
        "version": "1.0.0"
    }


@app.get("/health")
async def health():
    """Health check."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    logger.info(f"Iniciando servidor em {settings.host}:{settings.port}")
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=True
    )
