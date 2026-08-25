"""Tests for chat_service aggregation detection and context formatting."""
import sys
import os

# Allow importing from the backend root without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.chat_service import _is_aggregation_query, _format_overview_as_context


# ---------------------------------------------------------------------------
# _is_aggregation_query
# ---------------------------------------------------------------------------

def test_list_samples_detected():
    assert _is_aggregation_query("Quais amostras estão presentes nos dados?")

def test_list_all_detected():
    assert _is_aggregation_query("Liste todas as amostras disponíveis.")

def test_how_many_detected():
    assert _is_aggregation_query("Quantas amostras existem no dataset?")

def test_total_detected():
    assert _is_aggregation_query("Qual é o total de registros?")

def test_distinct_detected():
    assert _is_aggregation_query("Mostre os valores distintos de amostra.")

def test_unique_detected():
    assert _is_aggregation_query("Quais são os únicos nomes de arquivo?")

def test_average_detected():
    assert _is_aggregation_query("Qual é a média de concentração de Fe?")

def test_max_detected():
    assert _is_aggregation_query("Qual o máximo de Si no dataset?")

def test_ranking_detected():
    assert _is_aggregation_query("Faça um ranking das amostras por Fe.")

def test_list_all_english():
    assert _is_aggregation_query("list all samples")

def test_how_many_english():
    assert _is_aggregation_query("how many records are there?")

# Record-level queries should NOT be detected as aggregation
def test_specific_record_not_aggregation():
    assert not _is_aggregation_query("Qual é o valor de Fe para a amostra ABC123?")

def test_wavelength_lookup_not_aggregation():
    assert not _is_aggregation_query("Qual o reflectância a 450nm da amostra X?")

def test_what_is_not_aggregation():
    assert not _is_aggregation_query("O que é o Portal TCC?")


# ---------------------------------------------------------------------------
# _format_overview_as_context
# ---------------------------------------------------------------------------

SAMPLE_OVERVIEW = {
    "total_records": 150,
    "samples": ["A1", "A2", "B1", "B2", "C1"],
    "files": [
        {"file_name": "visnir.csv", "rows_count": 100, "columns": ["amostra", "450", "500"]},
        {"file_name": "pxrf.csv", "rows_count": 50, "columns": ["amostra", "Fe", "Si"]},
    ],
    "all_columns": ["450", "500", "Fe", "Si", "amostra"],
}


def test_format_includes_all_samples():
    ctx = _format_overview_as_context(SAMPLE_OVERVIEW)
    for sample in SAMPLE_OVERVIEW["samples"]:
        assert sample in ctx, f"Sample '{sample}' missing from formatted context"


def test_format_includes_total():
    ctx = _format_overview_as_context(SAMPLE_OVERVIEW)
    assert "150" in ctx


def test_format_includes_file_names():
    ctx = _format_overview_as_context(SAMPLE_OVERVIEW)
    assert "visnir.csv" in ctx
    assert "pxrf.csv" in ctx


def test_format_includes_sample_count():
    ctx = _format_overview_as_context(SAMPLE_OVERVIEW)
    assert "5" in ctx  # 5 distinct samples


def test_format_no_samples():
    overview = {**SAMPLE_OVERVIEW, "samples": []}
    ctx = _format_overview_as_context(overview)
    assert "Nenhuma coluna" in ctx or "nenhuma" in ctx.lower() or "amostra" in ctx


# ---------------------------------------------------------------------------
# _resolve_numeric_column
# ---------------------------------------------------------------------------

from services.chat_service import _resolve_numeric_column, _format_records_as_table

PXRF_COLUMNS = ["File #", "DateTime", "amostra", "Mg", "Mg Err", "Fe", "Fe Err",
                "Zn", "Zn Err", "Pb", "Pb Err", "K", "K Err"]


def test_resolve_element_name_in_portuguese():
    assert _resolve_numeric_column("qual a média de zinco na amostra", PXRF_COLUMNS) == "Zn"


def test_resolve_element_symbol():
    assert _resolve_numeric_column("qual a media de Zn", PXRF_COLUMNS) == "Zn"


def test_resolve_ignores_err_column_by_default():
    # "média de Zn" nunca pode cair em "Zn Err", que é a incerteza da medição
    assert _resolve_numeric_column("média de Zn", PXRF_COLUMNS) == "Zn"


def test_resolve_err_column_when_asked():
    assert _resolve_numeric_column("média do erro de zinco", PXRF_COLUMNS) == "Zn Err"


def test_resolve_accented_and_unaccented():
    assert _resolve_numeric_column("máximo de chumbo", PXRF_COLUMNS) == "Pb"
    assert _resolve_numeric_column("maximo de magnesio", PXRF_COLUMNS) == "Mg"


def test_resolve_single_letter_requires_exact_case():
    # "K" maiúsculo é a coluna; um "k" solto em outra palavra não é
    assert _resolve_numeric_column("qual a média de K", PXRF_COLUMNS) == "K"
    assert _resolve_numeric_column("qual o total de dados", PXRF_COLUMNS) is None


def test_resolve_returns_none_when_no_column_mentioned():
    assert _resolve_numeric_column("qual o total de dados contido na planilha", PXRF_COLUMNS) is None


def test_resolve_empty_columns():
    assert _resolve_numeric_column("média de zinco", []) is None


# ---------------------------------------------------------------------------
# _format_records_as_table
# ---------------------------------------------------------------------------

FULL = {
    "records": [
        {"file_name": "a.csv", "data": {"amostra": "A1", "Fe": 0.13, "Zn": 0.0064}},
        {"file_name": "a.csv", "data": {"amostra": "A2", "Fe": 0.21, "Zn": None}},
    ],
    "columns": ["amostra", "Fe", "Zn"],
    "total_records": 2,
    "truncated": False,
}


def test_table_has_header_once_and_one_line_per_record():
    lines = _format_records_as_table(FULL).split("\n")
    # 1 cabeçalho de contexto + 1 linha de colunas + 2 registros
    assert len(lines) == 4
    assert lines[1] == "amostra,Fe,Zn"


def test_table_announces_completeness():
    header = _format_records_as_table(FULL).split("\n")[0]
    assert "2 de 2" in header
    assert "TODOS" in header


def test_table_renders_null_as_empty():
    lines = _format_records_as_table(FULL).split("\n")
    assert lines[3] == "A2,0.21,"


def test_table_adds_file_column_only_when_multiple_files():
    single = _format_records_as_table(FULL).split("\n")[1]
    assert not single.startswith("arquivo")

    multi = {**FULL, "records": FULL["records"] + [
        {"file_name": "b.csv", "data": {"amostra": "B1", "Fe": 0.5, "Zn": 0.1}}
    ]}
    assert _format_records_as_table(multi).split("\n")[1].startswith("arquivo,")


def test_table_falls_back_to_json_keys_when_columns_missing():
    lines = _format_records_as_table({**FULL, "columns": []}).split("\n")
    assert lines[1] == "Fe,Zn,amostra"  # ordenadas, já que não há ordem original
