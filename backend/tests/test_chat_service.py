"""
Testes da camada de consulta e do serviço de chat.

Rodam sem banco e sem API: cobrem a tradução de filtros para SQL
parametrizado, a validação de colunas e a montagem do prompt.
"""
import sys
import os

# Permite importar da raiz de backend/ sem instalar o pacote.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from services.query_service import (
    ColunaDesconhecida,
    _clausula_filtros,
    _para_numero,
    _validar,
)

PXRF_COLUNAS = ["File #", "DateTime", "Name", "Application", "Zn", "Zn Err", "Fe", "Pb"]


# --- _para_numero ---

def test_numero_decimal_com_ponto():
    assert _para_numero("1.5") == 1.5


def test_numero_decimal_com_virgula():
    assert _para_numero("1,5") == 1.5


def test_numero_inteiro():
    assert _para_numero("42") == 42.0


def test_texto_nao_vira_numero():
    assert _para_numero("PlantsF1") is None


def test_booleano_nao_vira_numero():
    # True viraria 1.0 num float() ingênuo, transformando um filtro textual em
    # comparação numérica silenciosamente.
    assert _para_numero(True) is None


# --- _validar ---

def test_coluna_valida_passa():
    _validar(["Zn", "Fe"], PXRF_COLUNAS)


def test_coluna_invalida_lista_as_disponiveis():
    with pytest.raises(ColunaDesconhecida) as exc:
        _validar(["Zinco"], PXRF_COLUNAS)
    assert "Zinco" in str(exc.value)
    assert "Zn" in str(exc.value)  # o modelo precisa ver as opções para corrigir-se


# --- _clausula_filtros ---

def test_sem_filtros_nao_gera_clausula():
    assert _clausula_filtros(None, PXRF_COLUNAS) == ("", {})
    assert _clausula_filtros([], PXRF_COLUNAS) == ("", {})


def test_filtro_numerico_usa_parametros_vinculados():
    sql, params = _clausula_filtros(
        [{"coluna": "Fe", "operador": ">", "valor": "1.5"}], PXRF_COLUNAS
    )
    # O nome da coluna é valor vinculado (JSONB), não identificador interpolado.
    assert "Fe" not in sql
    assert params["fc0"] == "Fe"
    assert params["fv0"] == 1.5
    assert ">" in sql


def test_filtro_numerico_protege_contra_celula_de_texto():
    # Sem o jsonb_typeof, uma célula de texto faz o ::numeric estourar.
    sql, _ = _clausula_filtros(
        [{"coluna": "Fe", "operador": ">=", "valor": "0.1"}], PXRF_COLUNAS
    )
    assert "jsonb_typeof" in sql
    assert "::numeric" in sql


def test_filtro_de_texto_nao_faz_cast_numerico():
    sql, params = _clausula_filtros(
        [{"coluna": "Application", "operador": "=", "valor": "PlantsF1"}], PXRF_COLUNAS
    )
    assert "::numeric" not in sql
    assert params["fv0"] == "PlantsF1"


def test_operador_contem_vira_ilike_com_curingas():
    sql, params = _clausula_filtros(
        [{"coluna": "Name", "operador": "contém", "valor": "P1"}], PXRF_COLUNAS
    )
    assert "ILIKE" in sql
    assert params["fv0"] == "%P1%"


def test_filtros_multiplos_sao_combinados_com_and():
    sql, params = _clausula_filtros(
        [
            {"coluna": "Fe", "operador": ">", "valor": "1.0"},
            {"coluna": "Application", "operador": "contém", "valor": "Plants"},
        ],
        PXRF_COLUNAS,
    )
    assert " AND " in sql
    assert {"fc0", "fv0", "fc1", "fv1"} <= set(params)


def test_filtro_com_coluna_inexistente_e_rejeitado():
    with pytest.raises(ColunaDesconhecida):
        _clausula_filtros(
            [{"coluna": "Chumbo", "operador": ">", "valor": "1"}], PXRF_COLUNAS
        )


def test_operador_invalido_e_rejeitado():
    with pytest.raises(ValueError):
        _clausula_filtros(
            [{"coluna": "Fe", "operador": "DROP", "valor": "1"}], PXRF_COLUNAS
        )


# --- chat_service: montagem do prompt ---

from services.chat_service import _resumo_esquema, _texto_do_chunk

DESCRICAO = {
    "total_registros": 37,
    "arquivos": [{"nome": "pXRF-Exemplo.csv", "registros": 37}],
    "colunas": [
        {"nome": "Name", "tipo": "texto", "preenchidos": 37, "distintos": 37},
        {
            "nome": "Application",
            "tipo": "texto",
            "preenchidos": 37,
            "distintos": 2,
            "valores": ["PlantsF1", "SoilF2"],
        },
        {"nome": "Zn", "tipo": "numérica", "preenchidos": 37},
        {"nome": "Fe", "tipo": "numérica", "preenchidos": 37},
    ],
}


def test_resumo_lista_colunas_numericas():
    resumo = _resumo_esquema(DESCRICAO)
    assert "Zn" in resumo and "Fe" in resumo
    assert "Colunas numéricas (2)" in resumo


def test_resumo_expoe_valores_de_coluna_de_baixa_cardinalidade():
    # O modelo precisa saber que "PlantsF1" existe para montar um filtro.
    resumo = _resumo_esquema(DESCRICAO)
    assert "PlantsF1" in resumo and "SoilF2" in resumo


def test_resumo_omite_valores_de_coluna_de_alta_cardinalidade():
    # 37 nomes de amostra seriam ruído no prompt; só a cardinalidade importa.
    resumo = _resumo_esquema(DESCRICAO)
    assert "37 valores distintos" in resumo


def test_resumo_nao_contem_dados_apenas_estrutura():
    # Nenhum valor medido pode vazar para o prompt: quem calcula é o banco.
    resumo = _resumo_esquema(DESCRICAO)
    assert "0.0" not in resumo


def test_texto_do_chunk_aceita_string():
    assert _texto_do_chunk("olá") == "olá"


def test_texto_do_chunk_aceita_lista_multimodal():
    # O Gemini às vezes devolve content como lista de partes.
    assert _texto_do_chunk([{"text": "olá "}, {"text": "mundo"}]) == "olá mundo"


def test_texto_do_chunk_ignora_conteudo_nao_textual():
    assert _texto_do_chunk(None) == ""
    assert _texto_do_chunk([{"functionCall": {"name": "estatisticas"}}]) == ""


# --- routes.chat: tradução de falhas da API ---

from routes.chat import _mensagem_amigavel


def test_erro_de_cota_vira_mensagem_acionavel():
    bruto = Exception(
        "429 You exceeded your current quota. quota_metric: "
        "generativelanguage.googleapis.com/generate_content_free_tier_requests"
    )
    msg = _mensagem_amigavel(bruto)
    assert "Limite de uso" in msg
    assert "quota_metric" not in msg


def test_erro_de_chave_invalida_aponta_o_env():
    msg = _mensagem_amigavel(Exception("Invalid API key provided"))
    assert ".env" in msg


def test_erro_desconhecido_preserva_o_texto_original():
    # Falha inesperada não pode ser engolida: precisa chegar ao desenvolvedor.
    msg = _mensagem_amigavel(Exception("KeyError: 'grupo'"))
    assert "KeyError" in msg
