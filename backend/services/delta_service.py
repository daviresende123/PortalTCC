"""Serviço para gerenciar Delta Lake."""
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DeltaLakeService:
    """Serviço para interagir com Delta Lake."""

    def __init__(self, table_path: Path):
        """
        Inicializa o serviço Delta Lake.

        Args:
            table_path: Caminho para a tabela Delta
        """
        self.table_path = table_path
        self._ensure_directory()

    def _ensure_directory(self):
        """Garante que o diretório da tabela existe."""
        self.table_path.parent.mkdir(parents=True, exist_ok=True)

    def table_exists(self) -> bool:
        """Verifica se a tabela Delta já existe."""
        try:
            DeltaTable(str(self.table_path))
            return True
        except Exception:
            return False

    def save_dataframe(self, df: pd.DataFrame, mode: str = "append") -> int:
        """
        Salva um DataFrame no Delta Lake.

        Args:
            df: DataFrame pandas para salvar
            mode: Modo de escrita ('append', 'overwrite', 'error', 'ignore')

        Returns:
            Número de linhas salvas
        """
        try:
            logger.info(f"Salvando {len(df)} linhas no Delta Lake (modo: {mode})")

            # Escrever no Delta Lake
            write_deltalake(
                table_or_uri=str(self.table_path),
                data=df,
                mode=mode,
                overwrite_schema=True if mode == "overwrite" else False
            )

            logger.info("Dados salvos com sucesso no Delta Lake")
            return len(df)

        except Exception as e:
            logger.error(f"Erro ao salvar no Delta Lake: {str(e)}")
            raise

    def read_table(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Lê dados da tabela Delta.

        Args:
            limit: Limite de linhas a retornar (None = todas)

        Returns:
            DataFrame pandas com os dados
        """
        try:
            if not self.table_exists():
                return pd.DataFrame()

            dt = DeltaTable(str(self.table_path))
            df = dt.to_pandas()

            if limit:
                df = df.head(limit)

            return df

        except Exception as e:
            logger.error(f"Erro ao ler do Delta Lake: {str(e)}")
            raise

    def get_table_info(self) -> dict:
        """
        Retorna informações sobre a tabela Delta.

        Returns:
            Dicionário com informações da tabela
        """
        try:
            if not self.table_exists():
                return {
                    "exists": False,
                    "rows": 0,
                    "version": None
                }

            dt = DeltaTable(str(self.table_path))
            df = dt.to_pandas()

            return {
                "exists": True,
                "rows": len(df),
                "columns": list(df.columns),
                "version": dt.version()
            }

        except Exception as e:
            logger.error(f"Erro ao obter info da tabela: {str(e)}")
            return {"exists": False, "error": str(e)}
