#!/usr/bin/env python3
"""
Script para visualizar dados do PostgreSQL de forma amigável.
"""
import asyncio
import sys
import os

# Allow running from the backend/ directory without installing the package
sys.path.insert(0, os.path.dirname(__file__))

import asyncpg
from config import settings


def print_separator(char="=", length=80):
    print(char * length)


def print_header(text):
    print_separator()
    print(f"  {text}")
    print_separator()


def _asyncpg_url(sqlalchemy_url: str) -> str:
    """Convert a SQLAlchemy async URL to a plain asyncpg URL."""
    return sqlalchemy_url.replace("postgresql+asyncpg://", "postgresql://")


async def main():
    url = _asyncpg_url(settings.database_url)

    print("\n VISUALIZADOR DE DADOS - PORTAL TCC\n")

    try:
        conn = await asyncpg.connect(url)
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        print(f"  URL: {url}")
        sys.exit(1)

    try:
        # ------------------------------------------------------------------
        # 1. INFORMACOES GERAIS
        # ------------------------------------------------------------------
        print_header("INFORMACOES GERAIS")

        total_files = await conn.fetchval("SELECT COUNT(*) FROM files")
        total_records = await conn.fetchval("SELECT COUNT(*) FROM records")

        print(f"Arquivos enviados : {total_files}")
        print(f"Registros salvos  : {total_records}")

        if total_records == 0:
            print("\nNenhum dado encontrado.")
            print("Faca upload de um arquivo CSV primeiro.\n")
            return

        # Distinct columns across all records
        column_rows = await conn.fetch(
            "SELECT DISTINCT key FROM records, jsonb_object_keys(data) AS key ORDER BY key"
        )
        columns = [r["key"] for r in column_rows]
        print(f"Colunas presentes : {', '.join(columns)}")
        print()

        # ------------------------------------------------------------------
        # 2. UPLOADS RECENTES
        # ------------------------------------------------------------------
        print_header("UPLOADS RECENTES")

        files = await conn.fetch(
            """
            SELECT id, file_name, rows_count, columns_list, uploaded_at
            FROM files
            ORDER BY uploaded_at DESC
            LIMIT 10
            """
        )

        for f in files:
            ts = f["uploaded_at"].strftime("%d/%m/%Y %H:%M:%S")
            print(f"  [{f['id']}] {f['file_name']}")
            print(f"       Linhas   : {f['rows_count']}")
            print(f"       Colunas  : {', '.join(f['columns_list'])}")
            print(f"       Enviado  : {ts}")
            print()

        # ------------------------------------------------------------------
        # 3. AMOSTRA DE DADOS (ultimos 10 registros)
        # ------------------------------------------------------------------
        print_header("AMOSTRA DE DADOS (ultimos 10 registros)")

        sample = await conn.fetch(
            """
            SELECT data, uploaded_at
            FROM records
            ORDER BY uploaded_at DESC
            LIMIT 10
            """
        )

        for i, row in enumerate(sample, start=1):
            ts = row["uploaded_at"].strftime("%d/%m/%Y %H:%M:%S")
            data = dict(row["data"])
            print(f"  Registro {i}  ({ts})")
            for k, v in data.items():
                print(f"    {k}: {v}")
            print()

        # ------------------------------------------------------------------
        # 4. SERIES TEMPORAIS (TimescaleDB) — registros por hora
        # ------------------------------------------------------------------
        print_header("SERIES TEMPORAIS - Registros por hora")

        buckets = await conn.fetch(
            """
            SELECT
                time_bucket('1 hour', uploaded_at) AS bucket,
                COUNT(*) AS count
            FROM records
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT 20
            """
        )

        if buckets:
            for b in buckets:
                ts = b["bucket"].strftime("%d/%m/%Y %H:%M")
                print(f"  {ts}  ->  {b['count']} registro(s)")
        else:
            print("  (sem dados suficientes)")
        print()

        # ------------------------------------------------------------------
        # Resumo final
        # ------------------------------------------------------------------
        print_separator()
        print(f"Visualizacao concluida. Total: {total_records} registro(s).")
        print_separator()
        print()

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
