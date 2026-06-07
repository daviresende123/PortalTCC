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
