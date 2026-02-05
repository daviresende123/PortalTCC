"""Configurações da aplicação."""
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    """Configurações da aplicação carregadas do .env."""

    # Servidor
    host: str = "0.0.0.0"
    port: int = 8000

    # Delta Lake
    delta_table_path: str = "./data/delta_table"

    # Upload
    max_file_size_mb: int = 10
    allowed_extensions: str = "csv"

    # CORS
    frontend_url: str = "http://localhost:5500"

    class Config:
        env_file = ".env"
        case_sensitive = False

    @property
    def max_file_size_bytes(self) -> int:
        """Retorna o tamanho máximo em bytes."""
        return self.max_file_size_mb * 1024 * 1024

    @property
    def delta_path(self) -> Path:
        """Retorna o Path do Delta Lake."""
        return Path(self.delta_table_path)


# Instância global de configurações
settings = Settings()
