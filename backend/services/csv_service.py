"""Serviço para processar arquivos CSV."""
import pandas as pd
import numpy as np
from io import BytesIO, StringIO
import re
import logging

logger = logging.getLogger(__name__)


def _decode(file_content: bytes) -> str:
    """Decodifica bytes para string tentando utf-8-sig e latin-1."""
    try:
        return file_content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return file_content.decode("latin-1")


def _split_csv_line(line: str) -> list[str]:
    """Split a CSV line respecting quoted fields."""
    result = []
    current = []
    in_quotes = False

    for char in line:
        if char == '"':
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            result.append("".join(current).strip())
            current = []
        else:
            current.append(char)

    result.append("".join(current).strip())
    return result


def _convert_comma_decimals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada coluna não-numérica do DataFrame, tenta converter
    vírgula decimal para ponto e transformar em float.
    Se a maioria dos valores da coluna não converter, mantém como string.
    Abordagem genérica — não depende de nomes de colunas.
    """
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            continue

        # Tenta converter: troca vírgula por ponto, limpa aspas/espaços
        converted = (
            df[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.strip('" ')
        )
        numeric = pd.to_numeric(converted, errors="coerce")

        # Conta quantos valores não-nulos originais viraram número
        non_null_mask = df[col].notna() & (df[col].astype(str).str.strip() != "")
        if non_null_mask.sum() == 0:
            continue

        converted_ratio = numeric[non_null_mask].notna().sum() / non_null_mask.sum()

        # Se pelo menos 50% dos valores não-vazios converteram, aceita como numérico
        if converted_ratio >= 0.5:
            df[col] = numeric

    return df


class CSVService:
    """Serviço para processar arquivos CSV."""

    @staticmethod
    def detect_csv_type(file_content: bytes, filename: str) -> str:
        """
        Detecta o tipo de CSV baseado na estrutura das primeiras linhas.

        Regras:
        - Nix: primeira linha começa com 'sep='
        - Visnir: header começa com 'Wavelength'
        - pXRF: header começa com 'File #'
        - Genérico: nenhum dos anteriores

        Returns:
            'visnir', 'nix', 'pxrf' ou 'generic'
        """
        text = _decode(file_content)
        lines = text.strip().split("\n")
        if not lines:
            return "generic"

        first_line = lines[0].strip()

        if first_line.lower().startswith("sep="):
            logger.info("Tipo detectado: Nix (sep= na primeira linha)")
            return "nix"

        if first_line.startswith("Wavelength"):
            logger.info("Tipo detectado: Visnir (header começa com Wavelength)")
            return "visnir"

        if first_line.startswith("File #"):
            logger.info("Tipo detectado: pXRF (header começa com File #)")
            return "pxrf"

        logger.info("Tipo detectado: genérico")
        return "generic"

    @staticmethod
    def _parse_visnir(file_content: bytes) -> pd.DataFrame:
        """
        Parse CSV do tipo Visnir.
        Estrutura: formato largo onde a primeira coluna identifica a amostra
        e as demais são wavelengths. Decimais podem usar vírgula.
        """
        text = _decode(file_content)
        df = pd.read_csv(StringIO(text), header=0)

        # Primeira coluna é sempre o identificador da amostra
        df.rename(columns={df.columns[0]: "amostra"}, inplace=True)

        # Converter vírgulas decimais em todas as colunas (exceto amostra, que fica string)
        df = _convert_comma_decimals(df)

        logger.info(f"Visnir: {len(df)} amostras, {len(df.columns)-1} wavelengths")
        return df

    @staticmethod
    def _parse_nix(file_content: bytes) -> pd.DataFrame:
        """
        Parse CSV do tipo Nix.
        Estrutura: 3 linhas de metadados antes do header real.
        Decimais com vírgula + notação científica.
        Coluna 'User Color Name' é o identificador da amostra.
        """
        text = _decode(file_content)
        df = pd.read_csv(StringIO(text), skiprows=3, header=0)

        # Renomear coluna de amostra
        if "User Color Name" in df.columns:
            df.rename(columns={"User Color Name": "amostra"}, inplace=True)

        # Converter vírgulas decimais genericamente
        df = _convert_comma_decimals(df)

        logger.info(f"Nix: {len(df)} amostras, {len(df.columns)} colunas")
        return df

    @staticmethod
    def _parse_pxrf(file_content: bytes) -> pd.DataFrame:
        """
        Parse CSV do tipo pXRF.
        Estrutura: headers repetidos no meio do arquivo (linhas começando com 'File #'),
        cada bloco pode ter colunas diferentes. Unifica com super-set.
        '< LOD' substituído por 0. Decimais podem usar vírgula.
        Coluna 'Name' é o identificador da amostra.
        """
        text = _decode(file_content)
        lines = text.strip().split("\n")

        # Identificar linhas de header (começam com 'File #') e coletar super-set de colunas
        header_lines = []
        all_columns = []
        seen = set()
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("File #"):
                header_lines.append(stripped)
                cols = [c.strip() for c in _split_csv_line(stripped)]
                for c in cols:
                    if c and c not in seen:
                        all_columns.append(c)
                        seen.add(c)

        if not header_lines:
            raise ValueError("pXRF: nenhum header encontrado")

        # Associar cada linha de dados ao seu header (o header imediatamente anterior)
        rows = []
        current_header_cols = None
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("File #"):
                current_header_cols = [c.strip() for c in _split_csv_line(stripped)]
                continue
            if current_header_cols is None:
                continue

            values = _split_csv_line(stripped)
            row = {col: None for col in all_columns}
            for i, val in enumerate(values):
                if i < len(current_header_cols):
                    col_name = current_header_cols[i]
                    if col_name in seen:
                        row[col_name] = val
            rows.append(row)

        df = pd.DataFrame(rows, columns=all_columns)

        # Substituir '< LOD' (qualquer variação de espaço) por 0
        df = df.replace(re.compile(r"^\s*<\s*LOD\s*$"), "0")

        # Renomear coluna de amostra
        if "Name" in df.columns:
            df.rename(columns={"Name": "amostra"}, inplace=True)

        # Converter vírgulas decimais genericamente
        df = _convert_comma_decimals(df)

        logger.info(f"pXRF: {len(df)} linhas, {len(all_columns)} colunas no super-set")
        return df

    @staticmethod
    def validate_and_parse_csv(file_content: bytes, filename: str) -> tuple[pd.DataFrame, str]:
        """
        Valida e converte conteúdo CSV em DataFrame.

        Returns:
            Tupla (DataFrame pandas, csv_type)

        Raises:
            ValueError: Se o CSV for inválido
        """
        try:
            logger.info(f"Processando arquivo CSV: {filename}")

            csv_type = CSVService.detect_csv_type(file_content, filename)

            if csv_type == "visnir":
                df = CSVService._parse_visnir(file_content)
            elif csv_type == "nix":
                df = CSVService._parse_nix(file_content)
            elif csv_type == "pxrf":
                df = CSVService._parse_pxrf(file_content)
            else:
                df = pd.read_csv(BytesIO(file_content), sep=None, engine="python")
                df = _convert_comma_decimals(df)

            if df.empty:
                raise ValueError("O arquivo CSV está vazio")

            if len(df.columns) == 0:
                raise ValueError("O arquivo CSV não possui colunas")

            df = CSVService.clean_dataframe(df)

            logger.info(f"CSV processado ({csv_type}): {len(df)} linhas, {len(df.columns)} colunas")
            logger.info(f"Colunas: {list(df.columns)}")

            return df, csv_type

        except ValueError:
            raise
        except pd.errors.EmptyDataError:
            raise ValueError("O arquivo CSV está vazio")
        except pd.errors.ParserError as e:
            raise ValueError(f"Erro ao processar CSV: {str(e)}")
        except Exception as e:
            logger.error(f"Erro inesperado ao processar CSV: {str(e)}")
            raise ValueError(f"Erro ao processar arquivo: {str(e)}")

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e prepara o DataFrame."""
        df = df.dropna(how='all')
        df = df.dropna(axis=1, how='all')
        df.columns = df.columns.str.strip()
        df = df.reset_index(drop=True)
        return df
