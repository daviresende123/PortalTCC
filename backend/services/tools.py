"""
Ferramentas expostas ao modelo (function calling).

As descrições em ESQUEMAS são enviadas ao Gemini como especificação das
ferramentas: são efetivamente parte do prompt, e é por elas que o modelo
decide entre estatística exata e busca semântica. Ajuste-as com esse cuidado.

Os schemas são escritos à mão em vez de inferidos das assinaturas com @tool
porque o conversor do langchain-google-genai 2.0.8 emite arrays sem o campo
`items` e o Gemini rejeita a requisição inteira com
"function_declarations[...].items: missing field".
"""
import asyncio
import json
import logging

from config import settings
from services import query_service
from services.query_service import ColunaDesconhecida

logger = logging.getLogger(__name__)

# O valor do filtro é sempre string: o schema de function calling do Gemini não
# aceita união de tipos. A conversão para número acontece no query_service.
_FILTRO = {
    "type": "object",
    "description": "Uma condição sobre uma coluna.",
    "properties": {
        "coluna": {
            "type": "string",
            "description": "Nome exato da coluna, como aparece no dataset.",
        },
        "operador": {
            "type": "string",
            "enum": [">", ">=", "<", "<=", "=", "!=", "contém"],
            "description": "Comparação a aplicar. 'contém' faz busca parcial em texto.",
        },
        "valor": {
            "type": "string",
            "description": (
                "Valor de comparação, sempre como texto. Números em formato "
                "decimal com ponto (ex.: '1.5'); a conversão é automática."
            ),
        },
    },
    "required": ["coluna", "operador", "valor"],
}

_LISTA_COLUNAS = {"type": "array", "items": {"type": "string"}}
_LISTA_FILTROS = {"type": "array", "items": _FILTRO}


ESQUEMAS = [
    {
        "name": "descrever_dataset",
        "description": (
            "Descreve a estrutura dos dados carregados: quais arquivos existem, "
            "quantos registros há, quais são as colunas, o tipo de cada uma "
            "(numérica ou texto) e, para colunas de texto com poucos valores "
            "distintos, a lista desses valores. Use para confirmar nomes de "
            "colunas ou descobrir por quais colunas faz sentido agrupar."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "estatisticas",
        "description": (
            "Calcula estatística descritiva EXATA no banco de dados: contagem, "
            "média, mediana, moda (com sua frequência), mínimo, máximo, desvio "
            "padrão, percentis 25 e 75, e soma.\n\n"
            "Esta é a ferramenta correta para QUALQUER pergunta que envolva "
            "cálculo. Nunca calcule médias, medianas ou somas por conta própria "
            "a partir de registros individuais."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "colunas": {
                    **_LISTA_COLUNAS,
                    "description": (
                        "Nomes das colunas a analisar. Deixe vazio para incluir "
                        "TODAS as colunas numéricas de uma vez — custa o mesmo "
                        "que pedir uma só, então nunca faça uma chamada por coluna."
                    ),
                },
                "agrupar_por": {
                    "type": "string",
                    "description": (
                        "Coluna de texto pela qual quebrar o resultado. Ex.: "
                        "'Application' devolve a estatística separada por "
                        "aplicação. Deixe vazio para não agrupar."
                    ),
                },
                "filtros": {
                    **_LISTA_FILTROS,
                    "description": "Condições que restringem quais registros entram no cálculo.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "consultar_registros",
        "description": (
            "Busca registros individuais do dataset, com filtro e ordenação "
            "exatos. Use para perguntas sobre linhas específicas: qual amostra "
            "tem o maior valor de algo, quais registros ultrapassam um limite, "
            "os dados de uma amostra nomeada. Para cálculos sobre o conjunto, "
            "use estatisticas."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "colunas": {
                    **_LISTA_COLUNAS,
                    "description": "Quais colunas trazer. Deixe vazio para trazer todas.",
                },
                "filtros": {
                    **_LISTA_FILTROS,
                    "description": "Condições que os registros devem satisfazer.",
                },
                "ordenar_por": {
                    "type": "string",
                    "description": "Coluna usada na ordenação. Deixe vazio para a ordem original.",
                },
                "ordem": {
                    "type": "string",
                    "enum": ["asc", "desc"],
                    "description": "'desc' para maiores primeiro, 'asc' para menores.",
                },
                "limite": {
                    "type": "integer",
                    "description": f"Quantos registros retornar (máximo {query_service.LIMITE_MAXIMO_REGISTROS}).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "buscar_semantica",
        "description": (
            "Busca registros semelhantes a uma descrição em linguagem natural, "
            "por similaridade de significado. Use apenas para perguntas "
            "exploratórias e subjetivas, sem critério numérico claro — por "
            "exemplo 'quais amostras parecem contaminadas'. Para qualquer coisa "
            "com número, limite ou cálculo, prefira consultar_registros ou "
            "estatisticas, que são exatas."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "texto": {"type": "string", "description": "Descrição do que procurar."},
                "k": {"type": "integer", "description": "Quantos registros retornar."},
            },
            "required": ["texto"],
        },
    },
]


# --- Executores ---

async def _descrever_dataset() -> dict:
    return await query_service.descrever_dataset()


async def _estatisticas(
    colunas: list[str] | None = None,
    agrupar_por: str = "",
    filtros: list[dict] | None = None,
) -> dict:
    return await query_service.estatisticas(
        colunas=colunas or None,
        agrupar_por=agrupar_por or None,
        filtros=filtros or None,
    )


async def _consultar_registros(
    colunas: list[str] | None = None,
    filtros: list[dict] | None = None,
    ordenar_por: str = "",
    ordem: str = "desc",
    limite: int = 20,
) -> dict:
    return await query_service.consultar_registros(
        colunas=colunas or None,
        filtros=filtros or None,
        ordenar_por=ordenar_por or None,
        ordem=ordem,
        limite=limite,
    )


async def _buscar_semantica(texto: str, k: int = 10) -> dict:
    from services.embedding_service import get_vector_store

    retriever = get_vector_store().as_retriever(
        search_type="similarity", search_kwargs={"k": max(1, min(int(k or 10), 50))}
    )
    # A chamada de embedding não tem timeout próprio: sob rate limit (429) o
    # cliente entra em retry sem nunca desistir.
    try:
        docs = await asyncio.wait_for(
            retriever.ainvoke(texto), timeout=settings.retrieval_timeout_seconds
        )
    except asyncio.TimeoutError:
        return {
            "erro": (
                f"busca semântica excedeu {settings.retrieval_timeout_seconds}s "
                f"(possível limite de taxa da API de embeddings). Tente responder "
                f"com consultar_registros ou estatisticas."
            )
        }
    return {"encontrados": len(docs), "registros": [d.page_content for d in docs]}


_EXECUTORES = {
    "descrever_dataset": _descrever_dataset,
    "estatisticas": _estatisticas,
    "consultar_registros": _consultar_registros,
    "buscar_semantica": _buscar_semantica,
}


def rotulo(nome: str, argumentos: dict) -> str:
    """
    Texto mostrado ao usuário enquanto a ferramenta roda.

    Também mantém bytes fluindo no SSE, o que impede o timer de ociosidade do
    frontend de abortar uma requisição saudável.
    """
    if nome == "descrever_dataset":
        return "Verificando a estrutura dos dados…"
    if nome == "estatisticas":
        colunas = argumentos.get("colunas")
        alvo = f"{len(colunas)} coluna(s)" if colunas else "todas as colunas"
        grupo = argumentos.get("agrupar_por")
        return f"Calculando estatísticas de {alvo}{f', por {grupo}' if grupo else ''}…"
    if nome == "consultar_registros":
        return "Consultando registros no banco…"
    if nome == "buscar_semantica":
        return "Buscando registros semelhantes…"
    return f"Executando {nome}…"


async def executar(nome: str, argumentos: dict) -> str:
    """
    Executa uma ferramenta e devolve o resultado serializado.

    Erros não sobem como exceção: viram um resultado que o modelo lê para se
    corrigir na rodada seguinte. Se ele pedir "Zinco", recebe de volta a lista
    de colunas reais e tenta "Zn".
    """
    executor = _EXECUTORES.get(nome)
    if executor is None:
        resultado = {
            "erro": f"ferramenta '{nome}' não existe. "
                    f"Disponíveis: {', '.join(_EXECUTORES)}"
        }
    else:
        try:
            resultado = await executor(**(argumentos or {}))
        except ColunaDesconhecida as exc:
            logger.info(f"Ferramenta '{nome}' recebeu coluna inválida: {exc}")
            resultado = {"erro": str(exc)}
        except TypeError as exc:
            logger.info(f"Ferramenta '{nome}' recebeu argumentos inválidos: {exc}")
            resultado = {"erro": f"argumentos inválidos: {exc}"}
        except Exception as exc:
            logger.warning(f"Falha na ferramenta '{nome}' com {argumentos}: {exc}")
            resultado = {"erro": f"{type(exc).__name__}: {exc}"}

    return json.dumps(resultado, ensure_ascii=False, default=str)
