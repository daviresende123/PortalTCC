"""PostgreSQL service — replaces the former DeltaLakeService."""
import json
import math
import logging
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def get_dataset_overview() -> dict:
    """
    Return a full dataset summary by querying PostgreSQL directly.

    Creates its own session so it can be called from services that don't
    use FastAPI dependency injection (e.g. chat_service).

    Returns a dict with:
      - total_records: int
      - samples: list of all distinct 'amostra' values
      - files: list of {file_name, rows_count, columns}
      - all_columns: sorted list of every JSONB key seen across all records
    """
    from db.connection import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        total_records = (
            await session.execute(text("SELECT COUNT(*) FROM records"))
        ).scalar_one()

        samples_rows = (
            await session.execute(
                text("""
                    SELECT DISTINCT data->>'amostra'
                    FROM records
                    WHERE data->>'amostra' IS NOT NULL
                    ORDER BY 1
                """)
            )
        ).fetchall()
        samples = [r[0] for r in samples_rows]

        files_rows = (
            await session.execute(
                text("""
                    SELECT file_name, rows_count, columns_list
                    FROM files
                    ORDER BY uploaded_at
                """)
            )
        ).fetchall()
        files = [
            {"file_name": r[0], "rows_count": r[1], "columns": r[2]}
            for r in files_rows
        ]

        col_rows = (
            await session.execute(
                text("""
                    SELECT DISTINCT key
                    FROM records, jsonb_object_keys(data) AS key
                    ORDER BY key
                    LIMIT 500
                """)
            )
        ).fetchall()
        all_columns = [r[0] for r in col_rows]

    return {
        "total_records": total_records,
        "samples": samples,
        "files": files,
        "all_columns": all_columns,
    }


# ---------------------------------------------------------------------------
# Contexto completo do dataset
#
# Enquanto o dataset couber no contexto do LLM, mandar TODOS os registros é
# estritamente melhor do que recuperar os k mais parecidos: não há risco de o
# modelo responder com base em parte dos dados, e economiza a chamada de
# embedding da pergunta (uma ida à API do Google a menos por mensagem).
#
# O corte é por CÉLULAS (linhas x colunas), não por linhas: um pXRF típico tem
# 37 x 65 = 2.405 células e passa folgado, enquanto um Visnir em formato largo
# tem milhares de colunas de wavelength e estouraria o contexto com poucas
# linhas. Acima do limite, cai para busca vetorial + estatística em SQL.
# ---------------------------------------------------------------------------
MAX_CONTEXT_ROWS = 5_000
MAX_CONTEXT_CELLS = 150_000


async def get_all_records(
    max_rows: int = MAX_CONTEXT_ROWS,
    max_cells: int = MAX_CONTEXT_CELLS,
) -> dict:
    """
    Retorna TODOS os registros do banco, na ordem original de upload.

    Não lê o arquivo CSV: lê o JSONB já persistido em `records`. A ordem das
    colunas vem de `files.columns_list`, que preserva a ordem original do
    arquivo (JSONB não preserva ordem de chaves).

    Returns a dict with:
      - records: list of {"file_name": str, "data": dict}
      - columns: column names in original file order
      - total_records: total in the database
      - truncated: True when the dataset is too large to send in full
                   (in that case `records` comes back empty)
    """
    from db.connection import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        total = (
            await session.execute(text("SELECT COUNT(*) FROM records"))
        ).scalar_one()

        files_rows = (
            await session.execute(
                text("SELECT columns_list FROM files ORDER BY uploaded_at, id")
            )
        ).fetchall()

        # Super-set das colunas, preservando a ordem de cada arquivo
        columns: list[str] = []
        seen: set[str] = set()
        for row in files_rows:
            for col in row[0] or []:
                if col not in seen:
                    seen.add(col)
                    columns.append(col)

        n_cols = len(columns) or 1
        if total > max_rows or total * n_cols > max_cells:
            logger.info(
                f"Dataset grande demais para contexto completo: "
                f"{total} registros x {n_cols} colunas"
            )
            return {
                "records": [],
                "columns": columns,
                "total_records": total,
                "truncated": True,
            }

        # data::text porque o driver asyncpg devolve JSONB como string quando
        # a query é SQL cru (sem tipo declarado pelo SQLAlchemy)
        rows = (
            await session.execute(
                text("""
                    SELECT f.file_name, r.data::text
                    FROM records r
                    JOIN files f ON f.id = r.file_id
                    ORDER BY r.uploaded_at, r.id
                """)
            )
        ).fetchall()

    records = [
        {
            "file_name": r[0],
            "data": json.loads(r[1]) if isinstance(r[1], str) else r[1],
        }
        for r in rows
    ]

    return {
        "records": records,
        "columns": columns,
        "total_records": total,
        "truncated": False,
    }


async def get_column_stats(column: str) -> dict | None:
    """
    Calcula estatísticas de uma coluna numérica direto no PostgreSQL.

    Só considera valores que o JSONB guarda como número — o csv_service já
    converteu as colunas numéricas, e NaN virou null na gravação, então
    `jsonb_typeof(...) = 'number'` filtra exatamente os valores válidos.

    Returns None quando a coluna não tem nenhum valor numérico.
    """
    from db.connection import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        total = (
            await session.execute(text("SELECT COUNT(*) FROM records"))
        ).scalar_one()

        # CAST explícito: sem ele o operador `->` fica ambíguo entre as
        # sobrecargas jsonb->int e jsonb->text na hora de inferir o parâmetro
        row = (
            await session.execute(
                text("""
                    SELECT COUNT(*)          AS n,
                           AVG(v)            AS media,
                           MIN(v)            AS minimo,
                           MAX(v)            AS maximo,
                           STDDEV_SAMP(v)    AS desvio,
                           SUM(v)            AS soma
                    FROM (
                        SELECT (data ->> CAST(:col AS text))::numeric AS v
                        FROM records
                        WHERE jsonb_typeof(data -> CAST(:col AS text)) = 'number'
                    ) s
                """),
                {"col": column},
            )
        ).fetchone()

    if row is None or not row.n:
        return None

    return {
        "column": column,
        "count": row.n,
        "total_records": total,
        "media": float(row.media) if row.media is not None else None,
        "minimo": float(row.minimo) if row.minimo is not None else None,
        "maximo": float(row.maximo) if row.maximo is not None else None,
        "desvio": float(row.desvio) if row.desvio is not None else None,
        "soma": float(row.soma) if row.soma is not None else None,
    }


class DatabaseService:
    """Handles all database operations for CSV data storage."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_dataframe(self, df: pd.DataFrame, file_name: str) -> tuple[int, int]:
        """
        Persist a DataFrame to the database.

        Inserts one row into `files` (metadata) and one row per DataFrame
        row into `records` (data stored as JSONB).

        Returns:
            Tuple of (rows_saved, file_id).
        """
        columns = df.columns.tolist()
        rows_count = len(df)

        # Insert file metadata and retrieve the generated id
        result = await self.session.execute(
            text("""
                INSERT INTO files (file_name, rows_count, columns_list)
                VALUES (:file_name, :rows_count, :columns_list)
                RETURNING id
            """),
            {
                "file_name": file_name,
                "rows_count": rows_count,
                "columns_list": columns,
            },
        )
        file_id = result.scalar_one()

        # Build the batch of records, converting NaN → None for valid JSON
        records = [
            {
                "file_id": file_id,
                "data": json.dumps(
                    {
                        k: (None if isinstance(v, float) and math.isnan(v) else v)
                        for k, v in row.to_dict().items()
                    },
                    default=str,
                ),
            }
            for _, row in df.iterrows()
        ]

        await self.session.execute(
            text(
                "INSERT INTO records (file_id, data) "
                "VALUES (:file_id, CAST(:data AS jsonb))"
            ),
            records,
        )

        await self.session.commit()
        logger.info(
            f"Saved {rows_count} rows for file '{file_name}' (file_id={file_id})"
        )
        return rows_count, file_id

    async def get_stats(self) -> dict:
        """
        Return storage statistics.

        Mirrors the shape of the former get_table_info() response so that
        any existing consumer of /api/table-info keeps working.
        """
        count_row = (
            await self.session.execute(
                text("""
                    SELECT
                        (SELECT COUNT(*) FROM files)   AS total_files,
                        (SELECT COUNT(*) FROM records) AS total_records
                """)
            )
        ).fetchone()

        columns = [
            row[0]
            for row in (
                await self.session.execute(
                    text("""
                        SELECT DISTINCT key
                        FROM records, jsonb_object_keys(data) AS key
                        ORDER BY key
                        LIMIT 200
                    """)
                )
            ).fetchall()
        ]

        return {
            "exists": count_row.total_records > 0,
            "total_files": count_row.total_files,
            "total_records": count_row.total_records,
            "columns": columns,
        }
