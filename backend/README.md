# Backend — Portal TCC

API FastAPI que recebe arquivos CSV de espectrometria pXRF, trata e
persiste os dados em PostgreSQL/TimescaleDB, indexa-os em ChromaDB e os
disponibiliza a um chatbot RAG.

> **Para instalar e rodar o projeto, use o [README da raiz](../README.md).**
> Este documento cobre apenas detalhes internos do backend.

---

## Organização

| Módulo | Responsabilidade |
|---|---|
| `main.py` | Aplicação FastAPI, ciclo de vida e serviço do frontend estático |
| `config.py` | Configurações carregadas do `.env` via pydantic-settings |
| `db/connection.py` | Engine assíncrona, sessões e criação do schema |
| `routes/upload.py` | Recebimento e validação dos arquivos |
| `routes/chat.py` | Endpoints do chatbot, incluindo streaming SSE |
| `services/csv_service.py` | Detecção de formato e tratamento dos CSVs |
| `services/db_service.py` | Persistência e consultas agregadas |
| `services/embedding_service.py` | Geração de vetores e acesso ao ChromaDB |
| `services/chat_service.py` | Pipeline RAG |

---

## Modelo de dados

O schema é criado automaticamente no startup, por `init_db()`.

**`files`** — um registro por arquivo enviado.

| Coluna | Tipo |
|---|---|
| `id` | `SERIAL PRIMARY KEY` |
| `file_name` | `VARCHAR(255)` |
| `rows_count` | `INTEGER` |
| `columns_list` | `TEXT[]` |
| `uploaded_at` | `TIMESTAMPTZ` |

**`records`** — uma linha do CSV por registro, com os valores em JSONB.
Convertida em *hypertable* do TimescaleDB, particionada por `uploaded_at`.
A chave primária é composta (`id`, `uploaded_at`) porque o TimescaleDB
exige a coluna de tempo na chave.

Índices: GIN sobre `data` e B-tree sobre `file_id`.

Os vetores **não** ficam no PostgreSQL — são persistidos pelo ChromaDB no
diretório definido em `CHROMA_PERSIST_DIR`.

---

## Pipeline RAG

`chat_service.py` classifica cada pergunta antes de recuperar o contexto:

- **Consultas de agregação** (listar, contar, média, máximo, ranking,
  valores distintos) são detectadas por expressão regular. Nesses casos o
  contexto é montado a partir de um resumo completo obtido direto do
  PostgreSQL, complementado pela busca vetorial, e o prompt instrui o
  modelo a não truncar nem resumir a resposta.
- **Consultas sobre registros específicos** usam apenas busca por
  similaridade no ChromaDB (`k=10`).

O histórico é mantido em memória por `session_id`, limitado às 10 últimas
trocas enviadas ao modelo. Reiniciar a aplicação limpa as sessões.

Modelos configuráveis pelo `.env`: `LLM_MODEL` (padrão
`gemini-2.5-flash`), `EMBEDDING_MODEL` (padrão
`models/gemini-embedding-001`) e `LLM_TEMPERATURE` (padrão `0.3`).

---

## Comportamento sem chave de API

Quando `GOOGLE_API_KEY` está vazia:

- o upload funciona normalmente e os dados são persistidos no PostgreSQL;
- a geração de embeddings falha e é registrada como `WARNING`, sem
  interromper o upload;
- os endpoints de chat retornam uma mensagem explicando como configurar a
  chave, em vez de propagar a exceção.

---

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/api/upload` | Upload de CSV (campo `csvFile`, multipart) |
| `GET` | `/api/table-info` | Contagem de arquivos, registros e colunas |
| `POST` | `/api/chat` | Pergunta com resposta completa |
| `POST` | `/api/chat/stream` | Pergunta com resposta em SSE |
| `DELETE` | `/api/chat/session/{id}` | Limpa o histórico da sessão |
| `GET` | `/api/info` | Informações da API |
| `GET` | `/health` | Health check |

Documentação interativa em `/docs` e `/redoc`.

> O frontend é montado em `/` com `StaticFiles`, **depois** de todas as
> demais rotas. A ordem importa: como o Starlette resolve rotas na ordem de
> registro, montar `/` antes dos routers faria essa rota capturar todas as
> requisições e os endpoints `/api/*` deixariam de responder.

---

## Testes

```bash
python -m pytest tests/ -v
```

Cobrem a detecção de consultas de agregação e a formatação do contexto
enviado ao modelo.

---

## Variáveis de ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `HOST` | `0.0.0.0` | Interface do servidor |
| `PORT` | `8000` | Porta |
| `DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@localhost:5432/portaltcc` | Conexão com o PostgreSQL |
| `MAX_FILE_SIZE_MB` | `10` | Tamanho máximo do upload |
| `GOOGLE_API_KEY` | vazio | Chave do Gemini |
| `CHROMA_PERSIST_DIR` | `chroma_db` | Diretório de persistência do ChromaDB |
| `CHROMA_COLLECTION_NAME` | `portaltcc_records` | Nome da coleção |
| `LLM_MODEL` | `gemini-2.5-flash` | Modelo de linguagem |
| `EMBEDDING_MODEL` | `models/gemini-embedding-001` | Modelo de embeddings |
| `LLM_TEMPERATURE` | `0.3` | Temperatura do modelo |
| `FRONTEND_URL` | `http://localhost:5500` | Origem adicional liberada no CORS |

Rodando via Docker Compose, `DATABASE_URL` e `CHROMA_PERSIST_DIR` são
definidos pelo próprio `docker-compose.yml`.
