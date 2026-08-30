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


def _split_csv_line(line: str, sep: str = ",") -> list[str]:
    """Divide uma linha de CSV respeitando campos entre aspas."""
    result = []
    current = []
    in_quotes = False

    for char in line:
        if char == '"':
            in_quotes = not in_quotes
        elif char == sep and not in_quotes:
            result.append("".join(current).strip())
            current = []
        else:
            current.append(char)

    result.append("".join(current).strip())
    return result


def _detect_sep(line: str) -> str:
    """Detecta o separador de campos a partir da linha de header."""
    if line.count(";") > line.count(","):
        return ";"
    return ","


def _convert_comma_decimals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas com vírgula decimal em float.

    A coluna só é aceita como numérica se pelo menos metade dos valores não
    vazios converterem; caso contrário permanece como texto. Genérico: não
    depende de nomes de colunas.
    """
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            continue

        converted = (
            df[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.strip('" ')
        )
        numeric = pd.to_numeric(converted, errors="coerce")

        non_null_mask = df[col].notna() & (df[col].astype(str).str.strip() != "")
        if non_null_mask.sum() == 0:
            continue

        converted_ratio = numeric[non_null_mask].notna().sum() / non_null_mask.sum()

        if converted_ratio >= 0.5:
            df[col] = numeric

    return df


class CSVService:
    """Serviço para processar arquivos CSV."""

    @staticmethod
    def detect_csv_type(file_content: bytes, filename: str) -> str:
        """
        Detecta o tipo de CSV pela estrutura das primeiras linhas.

        - nix    : primeira linha começa com 'sep='
        - visnir : header começa com 'Wavelength'
        - pxrf   : header começa com 'File #'
        - generic: nenhum dos anteriores
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
        Formato largo: a primeira coluna identifica a amostra e as demais são
        wavelengths. Decimais podem usar vírgula.
        """
        text = _decode(file_content)
        df = pd.read_csv(StringIO(text), header=0, sep=None, engine="python")

        df.rename(columns={df.columns[0]: "amostra"}, inplace=True)
        df = _convert_comma_decimals(df)

        logger.info(f"Visnir: {len(df)} amostras, {len(df.columns)-1} wavelengths")
        return df

    @staticmethod
    def _parse_nix(file_content: bytes) -> pd.DataFrame:
        """
        Três linhas de metadados antes do header real, sendo a primeira a
        declaração do separador ("sep=;"). Decimais com vírgula e notação
        científica. A coluna 'User Color Name' identifica a amostra.
        """
        text = _decode(file_content)
        lines = text.strip().split("\n")
        sep = ","
        if lines and lines[0].lower().startswith("sep="):
            declared = lines[0].strip()[4:].strip()
            if declared in (";", "\t", "|"):
                sep = declared
        df = pd.read_csv(StringIO(text), skiprows=3, header=0, sep=sep)

        if "User Color Name" in df.columns:
            df.rename(columns={"User Color Name": "amostra"}, inplace=True)

        df = _convert_comma_decimals(df)

        logger.info(f"Nix: {len(df)} amostras, {len(df.columns)} colunas")
        return df

    @staticmethod
    def _parse_pxrf(file_content: bytes) -> pd.DataFrame:
        """
        Headers repetidos ao longo do arquivo (linhas iniciadas por 'File #'),
        cada bloco podendo ter colunas diferentes — unificadas aqui num
        super-set. '< LOD' vira 0 e a coluna 'Name' identifica a amostra.
        """
        text = _decode(file_content)
        lines = text.strip().split("\n")

        first_header = next((l.strip() for l in lines if l.strip().startswith("File #")), "")
        sep = _detect_sep(first_header)
        logger.info(f"pXRF: separador detectado = {repr(sep)}")

        # Coleta o super-set de colunas de todos os headers do arquivo.
        header_lines = []
        all_columns = []
        seen = set()
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("File #"):
                header_lines.append(stripped)
                cols = [c.strip() for c in _split_csv_line(stripped, sep)]
                for c in cols:
                    if c and c not in seen:
                        all_columns.append(c)
                        seen.add(c)

        if not header_lines:
            raise ValueError("pXRF: nenhum header encontrado")

        # Cada linha de dados segue o header imediatamente anterior a ela.
        rows = []
        current_header_cols = None
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("File #"):
                current_header_cols = [c.strip() for c in _split_csv_line(stripped, sep)]
                continue
            if current_header_cols is None:
                continue

            values = _split_csv_line(stripped, sep)
            row = {col: None for col in all_columns}
            for i, val in enumerate(values):
                if i < len(current_header_cols):
                    col_name = current_header_cols[i]
                    if col_name in seen:
                        row[col_name] = val
            rows.append(row)

        df = pd.DataFrame(rows, columns=all_columns)

        # '< LOD' (abaixo do limite de detecção) equivale a zero.
        df = df.replace(re.compile(r"^\s*<\s*LOD\s*$"), "0")

        if "Name" in df.columns:
            df.rename(columns={"Name": "amostra"}, inplace=True)

        df = _convert_comma_decimals(df)

        logger.info(f"pXRF: {len(df)} linhas, {len(all_columns)} colunas no super-set")
        return df

    @staticmethod
    def validate_and_parse_csv(file_content: bytes, filename: str) -> tuple[pd.DataFrame, str]:
        """
        Valida e converte o conteúdo do CSV em um DataFrame.

        Devolve a tupla (DataFrame, csv_type) e levanta ValueError se o
        arquivo for inválido.
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
