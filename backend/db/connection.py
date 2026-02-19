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
      - vector       (pgvector — vector similarity search for embeddings)

    Tables created:
      - files      : metadata for each uploaded CSV file
      - records    : individual CSV rows stored as JSONB; converted to a
                     TimescaleDB hypertable partitioned by uploaded_at
      - embeddings : vector embeddings linked to records (pgvector)
    """
    async with engine.begin() as conn:
        # ------------------------------------------------------------------
        # Extensions
        # ------------------------------------------------------------------
        await conn.execute(
            text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
        )
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

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

        # ------------------------------------------------------------------
        # embeddings table — pgvector (1536-dim, suitable for most models)
        # No FK to records: TimescaleDB hypertables do not support being
        # referenced by FK constraints from other tables.
        # ------------------------------------------------------------------
        await conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS embeddings (
                    id         BIGSERIAL    PRIMARY KEY,
                    record_id  BIGINT,
                    embedding  vector(1536),
                    model      VARCHAR(100),
                    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
                )
            """)
        )

        # IVFFlat index — create after the table has sufficient data.
        # Uncomment and run once you have rows in the embeddings table:
        # CREATE INDEX idx_embeddings_vector
        #   ON embeddings USING ivfflat (embedding vector_cosine_ops)
        #   WITH (lists = 100);

    logger.info("Database initialized successfully")
