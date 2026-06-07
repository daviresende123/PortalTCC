"""Database connection, session factory, and schema initialization."""
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
    """FastAPI dependency that yields an async database session."""
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    """
    Initialize PostgreSQL extensions, tables, and indexes.

    Extensions required:
      - timescaledb  (time-series partitioning for the records table)

    Tables created:
      - files   : metadata for each uploaded CSV file
      - records : individual CSV rows stored as JSONB; converted to a
                  TimescaleDB hypertable partitioned by uploaded_at

    Note: vector embeddings are stored externally in ChromaDB
    (see services/embedding_service.py), not in PostgreSQL.
    """
    async with engine.begin() as conn:
        # ------------------------------------------------------------------
        # Extensions
        # ------------------------------------------------------------------
        await conn.execute(
            text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
        )

        # ------------------------------------------------------------------
        # files table — one row per uploaded CSV file
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # records table — individual CSV rows as JSONB
        # PRIMARY KEY must include the time column for TimescaleDB
        # ------------------------------------------------------------------
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

        # Convert records to a TimescaleDB hypertable (idempotent)
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

    logger.info("Database initialized successfully")