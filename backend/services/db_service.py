"""
Persistência dos CSVs enviados.

Só a escrita, usada pelo fluxo de upload. A leitura analítica que o chatbot
consulta vive em query_service.py.
"""
import json
import math
import logging
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)


class DatabaseService:
    """Operações de escrita dos dados de CSV no banco."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_dataframe(self, df: pd.DataFrame, file_name: str) -> tuple[int, int]:
        """
        Salva um DataFrame: uma linha em `files` com os metadados e uma linha
        por registro em `records`, com os dados em JSONB.

        Devolve a tupla (linhas_salvas, file_id).
        """
        columns = df.columns.tolist()
        rows_count = len(df)

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

        # NaN vira None: JSON não tem representação para NaN.
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
            f"Salvas {rows_count} linhas de '{file_name}' (file_id={file_id})"
        )
        return rows_count, file_id

    async def get_stats(self) -> dict:
        """Estatísticas de armazenamento expostas por /api/table-info."""
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
