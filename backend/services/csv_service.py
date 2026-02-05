"""Serviço para processar arquivos CSV."""
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)


class CSVService:
    """Serviço para processar arquivos CSV."""

    @staticmethod
    def validate_and_parse_csv(file_content: bytes, filename: str) -> pd.DataFrame:
        """
        Valida e converte conteúdo CSV em DataFrame.

        Args:
            file_content: Conteúdo do arquivo em bytes
            filename: Nome do arquivo

        Returns:
            DataFrame pandas

        Raises:
            ValueError: Se o CSV for inválido
        """
        try:
            logger.info(f"Processando arquivo CSV: {filename}")

            # Tentar ler como CSV
            df = pd.read_csv(BytesIO(file_content))

            # Validar se tem dados
            if df.empty:
                raise ValueError("O arquivo CSV está vazio")

            # Validar se tem colunas
            if len(df.columns) == 0:
                raise ValueError("O arquivo CSV não possui colunas")

            logger.info(f"CSV processado: {len(df)} linhas, {len(df.columns)} colunas")
            logger.info(f"Colunas: {list(df.columns)}")

            return df

        except pd.errors.EmptyDataError:
            raise ValueError("O arquivo CSV está vazio")
        except pd.errors.ParserError as e:
            raise ValueError(f"Erro ao processar CSV: {str(e)}")
        except Exception as e:
            logger.error(f"Erro inesperado ao processar CSV: {str(e)}")
            raise ValueError(f"Erro ao processar arquivo: {str(e)}")

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e prepara o DataFrame.

        Args:
            df: DataFrame para limpar

        Returns:
            DataFrame limpo
        """
        # Remover linhas completamente vazias
        df = df.dropna(how='all')

        # Remover colunas completamente vazias
        df = df.dropna(axis=1, how='all')

        # Limpar nomes de colunas (remover espaços extras)
        df.columns = df.columns.str.strip()

        return df
