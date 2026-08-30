"""Configurações da aplicação."""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configurações da aplicação carregadas do .env."""

    # Servidor
    host: str = "0.0.0.0"
    port: int = 8000

    # PostgreSQL
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/portaltcc"

    # Upload
    max_file_size_mb: int = 10
    allowed_extensions: str = "csv"

    # CORS
    frontend_url: str = "http://localhost:5500"

    # Google AI
    google_api_key: str = ""

    # ChromaDB
    chroma_persist_dir: str = "chroma_db"
    chroma_collection_name: str = "portaltcc_records"

    # Embedding
    embedding_model: str = "models/gemini-embedding-001"

    # LLM
    llm_model: str = "gemini-2.5-flash"
    llm_temperature: float = 0.3
    # Sem timeout, uma chamada sob rate limit fica em retry indefinido.
    llm_timeout_seconds: int = 90
    llm_max_retries: int = 2
    retrieval_timeout_seconds: int = 30
    # Rodadas de ferramenta antes de o modelo ser obrigado a responder.
    chat_max_iteracoes: int = 4
    # Deve ser bem menor que o IDLE_TIMEOUT_MS do frontend (45s).
    sse_heartbeat_seconds: int = 10

    class Config:
        env_file = ".env"
        case_sensitive = False

    @property
    def max_file_size_bytes(self) -> int:
        """Retorna o tamanho máximo em bytes."""
        return self.max_file_size_mb * 1024 * 1024


settings = Settings()
