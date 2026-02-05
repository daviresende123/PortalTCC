# Backend - Portal TCC

API FastAPI para upload de arquivos CSV e armazenamento em Delta Lake.

## Instalação

1. Criar ambiente virtual:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

2. Instalar dependências:
```bash
pip install -r requirements.txt
```

3. Configurar variáveis de ambiente:
```bash
cp .env.example .env
# Editar .env conforme necessário
```

## Executar

```bash
python main.py
```

Ou usando uvicorn diretamente:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Endpoints

- `GET /` - Informações da API
- `GET /health` - Health check
- `POST /api/upload` - Upload de arquivo CSV
- `GET /api/table-info` - Informações da tabela Delta

## Documentação Interativa

Após iniciar o servidor, acesse:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Delta Lake

Os dados CSV são armazenados em formato Delta Lake no diretório `data/delta_table/`.
