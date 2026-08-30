"""
Camada de consulta analítica — o que as ferramentas do chat expõem ao modelo.

Estas funções são *parametrizadas*, não específicas de pergunta: o modelo
escolhe colunas, filtros e agrupamento, e o PostgreSQL calcula. É isso que
substitui o antigo roteamento por regex, onde cada formato novo de pergunta
exigia uma regra nova no código.

Sobre segurança: os dados estão em JSONB, então nome de coluna aqui é
*valor*, não identificador — `data ->> :col` é um parâmetro vinculado e não
existe superfície de injeção. Ainda assim toda coluna é validada contra as
colunas realmente presentes nos arquivos, para que o modelo receba um erro
útil ("coluna X não existe, disponíveis: ...") em vez de um resultado vazio
quando inventa um nome.
"""
import json
import logging
from typing import Any

from sqlalchemy import bindparam, text

logger = logging.getLogger(__name__)

# Colunas de texto com no máximo esta cardinalidade têm seus valores listados
# em descrever_dataset(). Acima disso só o número de distintos é informado —
# uma coluna "Name" com uma amostra por linha não ajuda no prompt.
MAX_DISTINTOS_LISTADOS = 25

# Teto de linhas devolvidas por consultar_registros(), independente do que o
# modelo pedir. O objetivo das ferramentas é responder perguntas, não despejar
# o dataset no contexto — para isso existe estatisticas().
LIMITE_MAXIMO_REGISTROS = 200

_OPERADORES = {">": ">", ">=": ">=", "<": "<", "<=": "<=", "=": "=", "!=": "!="}
_OPERADORES_TEXTO = {"contém", "contem"}


class ColunaDesconhecida(ValueError):
    """Coluna pedida pelo modelo não existe no dataset."""


def _para_numero(valor: Any) -> float | None:
    """Converte para float quando possível. As ferramentas recebem tudo como
    string (o schema de function calling do Gemini lida mal com unions), então
    é aqui que "1.5" vira 1.5 e "PlantsF1" continua texto."""
    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
        return float(valor)
    if isinstance(valor, str):
        try:
            return float(valor.strip().replace(",", "."))
        except ValueError:
            return None
    return None


async def colunas_conhecidas() -> list[str]:
    """Todas as colunas do dataset, na ordem original dos arquivos."""
    from db.connection import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        linhas = (
            await session.execute(
                text("SELECT columns_list FROM files ORDER BY uploaded_at, id")
            )
        ).fetchall()

    colunas: list[str] = []
    vistas: set[str] = set()
    for linha in linhas:
        for col in linha[0] or []:
            if col not in vistas:
                vistas.add(col)
                colunas.append(col)
    return colunas


def _validar(colunas: list[str], conhecidas: list[str]) -> None:
    """Levanta ColunaDesconhecida com a lista de opções — a mensagem volta ao
    modelo como resultado da ferramenta, e ele se corrige na rodada seguinte."""
    faltando = [c for c in colunas if c not in conhecidas]
    if faltando:
        raise ColunaDesconhecida(
            f"coluna(s) não encontrada(s): {', '.join(faltando)}. "
            f"Colunas disponíveis: {', '.join(conhecidas)}"
        )


def _clausula_filtros(
    filtros: list[dict] | None, conhecidas: list[str]
) -> tuple[str, dict]:
    """Traduz filtros estruturados em SQL parametrizado."""
    if not filtros:
        return "", {}

    _validar([f["coluna"] for f in filtros], conhecidas)

    partes: list[str] = []
    params: dict[str, Any] = {}

    for i, filtro in enumerate(filtros):
        coluna, operador, valor = filtro["coluna"], filtro["operador"], filtro["valor"]
        pc, pv = f"fc{i}", f"fv{i}"
        params[pc] = coluna

        if operador in _OPERADORES_TEXTO:
            partes.append(f"(r.data ->> CAST(:{pc} AS text)) ILIKE :{pv}")
            params[pv] = f"%{valor}%"
            continue

        if operador not in _OPERADORES:
            raise ValueError(
                f"operador '{operador}' inválido. "
                f"Use um de: {', '.join(list(_OPERADORES) + ['contém'])}"
            )

        numero = _para_numero(valor)
        if numero is None:
            # Comparação textual: o valor não é número, então a coluna também
            # não deve ser tratada como tal.
            partes.append(
                f"(r.data ->> CAST(:{pc} AS text)) {_OPERADORES[operador]} :{pv}"
            )
            params[pv] = str(valor)
        else:
            # jsonb_typeof antes do cast: sem isso uma célula de texto na
            # coluna faz o ::numeric estourar e derruba a consulta inteira.
            partes.append(
                f"jsonb_typeof(r.data -> CAST(:{pc} AS text)) = 'number' "
                f"AND (r.data ->> CAST(:{pc} AS text))::numeric "
                f"{_OPERADORES[operador]} :{pv}"
            )
            params[pv] = numero

    return " AND ".join(f"({p})" for p in partes), params


def _where(clausula: str) -> str:
    return f"WHERE {clausula}" if clausula else ""


# ---------------------------------------------------------------------------
# Ferramenta 1 — descrever_dataset
# ---------------------------------------------------------------------------

async def descrever_dataset() -> dict:
    """
    Estrutura do dataset: arquivos, colunas, tipos e valores distintos.

    O resultado também é injetado no prompt do sistema a cada pergunta, para
    o modelo já saber que existe uma coluna "Zn" sem gastar uma rodada de
    ferramenta só para descobrir isso.
    """
    from db.connection import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        total = (
            await session.execute(text("SELECT COUNT(*) FROM records"))
        ).scalar_one()

        arquivos = [
            {"nome": r[0], "registros": r[1]}
            for r in (
                await session.execute(
                    text(
                        "SELECT file_name, rows_count FROM files "
                        "ORDER BY uploaded_at, id"
                    )
                )
            ).fetchall()
        ]

        # Um único passe por todas as células classifica todas as colunas.
        tipos = {
            r[0]: {"numericos": r[1], "textos": r[2]}
            for r in (
                await session.execute(
                    text("""
                        SELECT key,
                               COUNT(*) FILTER (WHERE jsonb_typeof(value) = 'number'),
                               COUNT(*) FILTER (WHERE jsonb_typeof(value) = 'string')
                        FROM records, jsonb_each(data)
                        GROUP BY key
                    """)
                )
            ).fetchall()
        }

        # Valores distintos, mas só das colunas de texto de baixa cardinalidade.
        distintos: dict[str, list[str]] = {}
        cardinalidade: dict[str, int] = {}
        for chave, valor, _freq, n_distintos in (
            await session.execute(
                text("""
                    WITH pares AS (
                        SELECT key, value #>> '{}' AS v
                        FROM records, jsonb_each(data)
                        WHERE jsonb_typeof(value) = 'string'
                    ),
                    card AS (
                        SELECT key, COUNT(DISTINCT v) AS n FROM pares GROUP BY key
                    )
                    SELECT p.key, p.v, COUNT(*) AS freq, c.n
                    FROM pares p JOIN card c ON c.key = p.key
                    GROUP BY p.key, p.v, c.n
                    ORDER BY p.key, freq DESC
                """)
            )
        ).fetchall():
            cardinalidade[chave] = n_distintos
            if n_distintos <= MAX_DISTINTOS_LISTADOS:
                distintos.setdefault(chave, []).append(valor)

    ordem = await colunas_conhecidas()
    colunas = []
    for nome in ordem:
        info = tipos.get(nome, {"numericos": 0, "textos": 0})
        numerica = info["numericos"] >= info["textos"] and info["numericos"] > 0
        entrada: dict[str, Any] = {
            "nome": nome,
            "tipo": "numérica" if numerica else ("texto" if info["textos"] else "vazia"),
            "preenchidos": info["numericos"] + info["textos"],
        }
        if not numerica and nome in cardinalidade:
            entrada["distintos"] = cardinalidade[nome]
            if nome in distintos:
                entrada["valores"] = distintos[nome]
        colunas.append(entrada)

    return {"total_registros": total, "arquivos": arquivos, "colunas": colunas}


# ---------------------------------------------------------------------------
# Ferramenta 2 — estatisticas
# ---------------------------------------------------------------------------

async def estatisticas(
    colunas: list[str] | None = None,
    agrupar_por: str | None = None,
    filtros: list[dict] | None = None,
) -> dict:
    """
    Estatística descritiva calculada em SQL, para uma, várias ou todas as
    colunas numéricas, opcionalmente agrupada e/ou filtrada.

    Um único passe com jsonb_each cobre todas as colunas de uma vez: pedir
    "todas" custa a mesma varredura que pedir uma.
    """
    from db.connection import AsyncSessionLocal

    conhecidas = await colunas_conhecidas()
    if colunas:
        _validar(colunas, conhecidas)
    if agrupar_por:
        _validar([agrupar_por], conhecidas)

    clausula, params = _clausula_filtros(filtros, conhecidas)

    # Grupo é sempre texto e nunca NULL: o LEFT JOIN com a CTE da moda usa
    # USING(grupo, key), e NULL não casa com NULL em join.
    if agrupar_por:
        expr_grupo = "COALESCE(r.data ->> CAST(:grp AS text), '(vazio)')"
        params["grp"] = agrupar_por
    else:
        expr_grupo = "''"

    filtro_colunas = "AND e.key IN :cols" if colunas else ""

    sql = f"""
        WITH base AS (
            SELECT r.data AS data, {expr_grupo} AS grupo
            FROM records r
            {_where(clausula)}
        ),
        valores AS (
            SELECT b.grupo, e.key, (e.value #>> '{{}}')::numeric AS v
            FROM base b, jsonb_each(b.data) e
            WHERE jsonb_typeof(e.value) = 'number' {filtro_colunas}
        ),
        agg AS (
            SELECT grupo, key,
                   COUNT(v)                                          AS n,
                   AVG(v)                                            AS media,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY v)    AS mediana,
                   MIN(v)                                            AS minimo,
                   MAX(v)                                            AS maximo,
                   STDDEV_SAMP(v)                                    AS desvio,
                   percentile_cont(0.25) WITHIN GROUP (ORDER BY v)   AS p25,
                   percentile_cont(0.75) WITHIN GROUP (ORDER BY v)   AS p75,
                   SUM(v)                                            AS soma
            FROM valores GROUP BY grupo, key
        ),
        moda AS (
            SELECT DISTINCT ON (grupo, key)
                   grupo, key, v AS moda, COUNT(*) AS moda_freq
            FROM valores
            GROUP BY grupo, key, v
            ORDER BY grupo, key, COUNT(*) DESC, v
        )
        SELECT a.grupo, a.key, a.n, a.media, a.mediana, a.minimo, a.maximo,
               a.desvio, a.p25, a.p75, a.soma, m.moda, m.moda_freq
        FROM agg a LEFT JOIN moda m USING (grupo, key)
        ORDER BY a.grupo, a.key
    """

    stmt = text(sql)
    if colunas:
        stmt = stmt.bindparams(bindparam("cols", expanding=True))
        params["cols"] = colunas

    async with AsyncSessionLocal() as session:
        total_base = (
            await session.execute(
                text(f"SELECT COUNT(*) FROM records r {_where(clausula)}"),
                {k: v for k, v in params.items() if k.startswith("f")},
            )
        ).scalar_one()
        linhas = (await session.execute(stmt, params)).fetchall()

    def num(v):
        return None if v is None else float(v)

    resultados = []
    for r in linhas:
        item = {
            "coluna": r.key,
            "n": r.n,
            "media": num(r.media),
            "mediana": num(r.mediana),
            "moda": num(r.moda),
            "moda_frequencia": r.moda_freq,
            "minimo": num(r.minimo),
            "maximo": num(r.maximo),
            "desvio_padrao": num(r.desvio),
            "p25": num(r.p25),
            "p75": num(r.p75),
            "soma": num(r.soma),
        }
        if agrupar_por:
            item = {"grupo": r.grupo, **item}
        resultados.append(item)

    saida: dict[str, Any] = {
        "registros_considerados": total_base,
        "estatisticas": resultados,
    }
    if agrupar_por:
        saida["agrupado_por"] = agrupar_por
    if not resultados:
        saida["aviso"] = (
            "Nenhum valor numérico encontrado para essas colunas/filtros."
        )
    return saida


# ---------------------------------------------------------------------------
# Ferramenta 3 — consultar_registros
# ---------------------------------------------------------------------------

async def consultar_registros(
    colunas: list[str] | None = None,
    filtros: list[dict] | None = None,
    ordenar_por: str | None = None,
    ordem: str = "desc",
    limite: int = 20,
) -> dict:
    """Registros individuais, filtrados e ordenados."""
    from db.connection import AsyncSessionLocal

    conhecidas = await colunas_conhecidas()
    if colunas:
        _validar(colunas, conhecidas)
    if ordenar_por:
        _validar([ordenar_por], conhecidas)

    clausula, params = _clausula_filtros(filtros, conhecidas)
    limite = max(1, min(int(limite or 20), LIMITE_MAXIMO_REGISTROS))
    direcao = "DESC" if str(ordem).lower().startswith("desc") else "ASC"

    if ordenar_por:
        params["ord"] = ordenar_por
        # O CASE ordena numericamente quando a célula é número e cai para
        # texto quando não é, sem precisar saber o tipo da coluna de antemão.
        order_by = f"""
            ORDER BY CASE
                       WHEN jsonb_typeof(r.data -> CAST(:ord AS text)) = 'number'
                       THEN (r.data ->> CAST(:ord AS text))::numeric
                     END {direcao} NULLS LAST,
                     (r.data ->> CAST(:ord AS text)) {direcao} NULLS LAST
        """
    else:
        order_by = "ORDER BY r.uploaded_at, r.id"

    params["lim"] = limite

    async with AsyncSessionLocal() as session:
        total = (
            await session.execute(
                text(f"SELECT COUNT(*) FROM records r {_where(clausula)}"),
                {k: v for k, v in params.items() if k.startswith("f")},
            )
        ).scalar_one()

        linhas = (
            await session.execute(
                text(f"""
                    SELECT f.file_name, r.data::text
                    FROM records r JOIN files f ON f.id = r.file_id
                    {_where(clausula)}
                    {order_by}
                    LIMIT :lim
                """),
                params,
            )
        ).fetchall()

    registros = []
    for nome_arquivo, bruto in linhas:
        dados = json.loads(bruto) if isinstance(bruto, str) else bruto
        if colunas:
            dados = {c: dados.get(c) for c in colunas}
        registros.append({"arquivo": nome_arquivo, **dados})

    return {
        "total_correspondente": total,
        "retornados": len(registros),
        "registros": registros,
    }
