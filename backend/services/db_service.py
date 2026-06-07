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
