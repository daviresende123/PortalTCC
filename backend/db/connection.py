"""Conexão com o banco, fábrica de sessões e criação do schema."""
import logging
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy import text
from config import settings

logger = logging.getLogger(__name__)

engine = create_async_engine(settings.database_url, echo=False, pool_pre_ping=True)

AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def get_db():
    """Dependência do FastAPI que fornece uma sessão assíncrona."""
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    """
    Cria a extensão TimescaleDB, as tabelas e os índices.

    - files   : metadados de cada CSV enviado
    - records : linhas do CSV em JSONB, como hypertable particionada por
                uploaded_at

    Os vetores de embedding ficam no ChromaDB (services/embedding_service.py),
    não no PostgreSQL.
    """
    async with engine.begin() as conn:
        await conn.execute(
            text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
        )

        await conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS files (
                    id           SERIAL PRIMARY KEY,
                    file_name    VARCHAR(255)  NOT NULL,
                    rows_count   INTEGER       NOT NULL,
                    columns_list TEXT[]        NOT NULL,
                    uploaded_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
                )
            """)
        )

        # A chave primária precisa incluir a coluna de tempo (TimescaleDB).
        await conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS records (
                    id          BIGSERIAL,
                    file_id     INTEGER     REFERENCES files(id) ON DELETE CASCADE,
                    data        JSONB       NOT NULL,
                    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (id, uploaded_at)
                )
            """)
        )

        await conn.execute(
            text(
                "SELECT create_hypertable("
                "  'records', 'uploaded_at', if_not_exists => TRUE"
                ")"
            )
        )

        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_records_data "
                "ON records USING GIN (data)"
            )
        )
        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_records_file_id "
                "ON records(file_id)"
            )
        )

    logger.info("Banco de dados inicializado")