# Portal TCC

Portal web com chatbot RAG para análise de dados de sensores de solo.

O equipamento mede a composição química de amostras e exporta os
resultados em arquivos CSV. Este sistema recebe esses arquivos, trata os
dados, armazena em banco, indexa semanticamente e permite que você faça
perguntas sobre eles em português, conversando com um chatbot.

> Trabalho de Conclusão de Curso — Ciência da Computação

**Tecnologias:** FastAPI · PostgreSQL + TimescaleDB · ChromaDB · LangChain · Google Gemini

---

## Sumário

- [O que o sistema faz](#o-que-o-sistema-faz)
- [Antes de começar: instalando o Docker](#antes-de-começar-instalando-o-docker)
- [Instalação passo a passo](#instalação-passo-a-passo)
- [Gerando a chave da API do Google](#gerando-a-chave-da-api-do-google)
- [Testando o sistema](#testando-o-sistema)
- [Como funciona por dentro](#como-funciona-por-dentro)
- [Comandos do dia a dia](#comandos-do-dia-a-dia)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Endpoints da API](#endpoints-da-api)
- [Solução de problemas](#solução-de-problemas)
- [Rodando sem Docker](#rodando-sem-docker)

---

## O que o sistema faz

O sistema tem duas telas:

**Upload** — você envia um arquivo CSV do espectrômetro. O sistema
identifica sozinho o formato do arquivo, trata as particularidades daquele
equipamento e grava tudo no banco de dados.

**Chat** — você pergunta em linguagem natural sobre os dados que enviou, e
o chatbot responde consultando o que está no banco. Perguntas como *"quais
amostras estão presentes?"*, *"qual a média de ferro?"* ou *"quantos
registros foram carregados?"*.

Formatos de CSV reconhecidos automaticamente:

| Formato | Como é identificado | Tratamento aplicado |
|---|---|---|
| **pXRF** | header começa com `File #` | unifica headers repetidos no meio do arquivo, substitui `< LOD` por 0 |
| **Nix** | primeira linha começa com `sep=` | pula as 3 linhas de metadados, usa o separador declarado |
| **Visnir** | header começa com `Wavelength` | formato largo, primeira coluna vira o identificador da amostra |
| **Genérico** | qualquer outro CSV | detecção automática de separador |

Em todos eles, decimais escritos com vírgula são convertidos para ponto.

---

## Antes de começar: instalando o Docker

Você **não** precisa instalar Python, PostgreSQL, TimescaleDB nem
biblioteca nenhuma. Tudo isso roda dentro de containers. O único
pré-requisito é o Docker.

### Windows

1. Baixe o [Docker Desktop](https://www.docker.com/products/docker-desktop/).
2. Execute o instalador e **mantenha marcada a opção "Use WSL 2 instead of
   Hyper-V"**.
3. Reinicie o computador quando for solicitado.
4. Abra o Docker Desktop e espere o ícone da baleia, no canto inferior
   esquerdo, ficar verde com a mensagem *Engine running*. **O Docker
   Desktop precisa estar aberto sempre que você for rodar o projeto.**

Use o **PowerShell** para os comandos deste guia.

### macOS

1. Baixe o [Docker Desktop](https://www.docker.com/products/docker-desktop/),
   escolhendo a versão correta: **Apple Silicon** (Macs M1/M2/M3/M4) ou
   **Intel**.
2. Arraste o Docker para a pasta Aplicativos e abra-o.
3. Espere o ícone da baleia aparecer na barra superior indicando que está
   rodando.

Use o **Terminal** para os comandos deste guia.

### Linux (Ubuntu/Debian)

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

O segundo comando permite usar o Docker sem `sudo`. **Faça logout e login
novamente** para que ele tenha efeito.

### Confirmando que deu certo

Em qualquer sistema, rode:

```bash
docker --version
docker compose version
```

Você deve ver algo como `Docker version 27.x.x` e `Docker Compose version
v2.x.x`. Se aparecer *"command not found"* ou *"cannot connect to the
Docker daemon"*, o Docker não está instalado ou não está aberto.

---

## Instalação passo a passo

### 1. Baixe o projeto

```bash
git clone https://github.com/daviresende123/PortalTCC.git
cd PortalTCC
```

Se você não tem Git instalado, dá para baixar o `.zip` direto pelo botão
verde **Code → Download ZIP** na página do repositório, extrair e abrir um
terminal dentro da pasta extraída.

### 2. Crie o arquivo de configuração

**Linux, macOS ou WSL:**

```bash
cp .env.example .env
```

**Windows (PowerShell):**

```powershell
copy .env.example .env
```

Esse comando cria o arquivo `.env`, que guarda a configuração do sistema.
Ele já vem com tudo preenchido — **só a chave do Google fica em branco**.

### 3. (Opcional agora) Coloque a chave do Google

Abra o `.env` em qualquer editor de texto e cole sua chave na linha:

```
GOOGLE_API_KEY=
```

Ficando assim:

```
GOOGLE_API_KEY=cole-sua-chave-aqui
```

A próxima seção explica como gerar essa chave. **Você pode pular este
passo por enquanto:** o sistema sobe sem a chave, o envio de CSV funciona
normalmente, e só o chat fica indisponível — ele vai responder com um
aviso explicando como configurar. Assim você conhece a interface antes de
decidir gerar a chave.

### 4. Suba o sistema

```bash
docker compose up
```

**A primeira execução demora alguns minutos**, porque o Docker precisa
baixar as imagens e instalar as dependências. É normal ver muitas linhas
de log passando. As execuções seguintes sobem em segundos.

Você saberá que está pronto quando aparecer:

```
backend-1  | INFO: Database ready
backend-1  | INFO: Uvicorn running on http://0.0.0.0:8000
```

### 5. Acesse

Abra o navegador em:

### **http://localhost:8000**

Pronto. O sistema inteiro — interface e API — está rodando nesse endereço.

Para parar, volte ao terminal e pressione **Ctrl+C**.

---

## Gerando a chave da API do Google

O chatbot usa o Google Gemini, que precisa de uma chave de API. Ela é
gratuita e leva menos de um minuto para ser criada.

1. Acesse **[aistudio.google.com/apikey](https://aistudio.google.com/apikey)**
2. Entre com sua conta Google.
3. Clique em **Create API key** (ou *Criar chave de API*).
4. Copie a chave gerada.
5. Cole no arquivo `.env`, na linha `GOOGLE_API_KEY=`, sem aspas e sem
   espaços.
6. Aplique a mudança:

```bash
docker compose restart backend
```

> A chave é pessoal. O arquivo `.env` já está configurado para nunca ser
> enviado ao GitHub, então não há risco de commitá-la por engano.

---

## Testando o sistema

O repositório já vem com um arquivo de exemplo, com **dados reais de
pXRF**, para você testar imediatamente sem precisar de arquivo próprio:

```
Exemplos/pXRF-Exemplo.csv
```

São 37 análises com 65 colunas de composição química, cobrindo 13 amostras
distintas.

### Roteiro

**1. Envie o arquivo**

Em http://localhost:8000, clique em **Selecionar Arquivo** (ou arraste o
arquivo para a área tracejada), escolha o `Exemplos/pXRF-Exemplo.csv` e
clique em **Enviar para Banco de Dados**.

Deve aparecer a mensagem *"Arquivo enviado e processado com sucesso!"*.

**2. Converse com os dados**

Clique na aba **Chat** e experimente perguntar:

- *Quais amostras estão presentes nos dados?*
- *Quantos registros foram carregados?*
- *Qual é a média de Fe nas amostras?*
- *Liste todas as colunas disponíveis.*
- *Qual amostra tem a maior concentração de Ca?*

As respostas aparecem em streaming, palavra por palavra, como num chat
comum.

### Limites

- Apenas arquivos com extensão `.csv`
- Tamanho máximo de **10 MB** por arquivo
- Você pode enviar vários arquivos; os dados se acumulam e o chat consulta
  todos eles

---

## Como funciona por dentro

Entender o caminho que o dado percorre ajuda a interpretar o que acontece
em cada tela.

### Quando você envia um CSV

```
CSV → detecção do formato → tratamento → PostgreSQL → embeddings → ChromaDB
```

1. **Detecção** — o sistema lê as primeiras linhas e identifica se é pXRF,
   Nix, Visnir ou genérico.
2. **Tratamento** — aplica as regras daquele formato: unificação de
   headers repetidos, substituição de `< LOD` por 0, conversão de vírgula
   decimal, padronização da coluna de identificação da amostra.
3. **Persistência** — os metadados vão para a tabela `files` e cada linha
   do CSV vira um registro JSONB na tabela `records`, que é uma
   *hypertable* do TimescaleDB particionada por data de envio.
4. **Indexação** — cada registro é convertido em texto, transformado em
   vetor pelo modelo de embeddings do Google e guardado no ChromaDB, em
   lotes de 100 para respeitar os limites da API.

### Quando você faz uma pergunta

O chatbot usa **RAG** (*Retrieval-Augmented Generation*): em vez de o
modelo de linguagem "chutar" com base no que ele aprendeu no treinamento,
ele recebe junto com a pergunta os dados reais que você enviou.

O sistema classifica sua pergunta em dois tipos:

- **Pergunta sobre registros específicos** — busca por similaridade no
  ChromaDB e recupera os 10 registros mais relevantes.
- **Pergunta de agregação** (listar, contar, média, máximo, ranking...) —
  consulta o PostgreSQL diretamente para montar um resumo completo do
  conjunto de dados, garantindo que nenhuma amostra fique de fora, e
  complementa com a busca vetorial.

O contexto recuperado, mais as últimas 10 trocas da conversa, são enviados
ao Gemini, que responde em português.

### Por que dois bancos?

O **PostgreSQL com TimescaleDB** guarda o dado estruturado e responde bem
a perguntas exatas — contagens, listas completas, agregações. O
**ChromaDB** guarda os vetores e responde bem a perguntas por semelhança
de significado. O sistema usa cada um onde ele é melhor.

---

## Comandos do dia a dia

Todos devem ser executados dentro da pasta do projeto.

| O que você quer | Comando |
|---|---|
| Subir o sistema | `docker compose up` |
| Subir em segundo plano | `docker compose up -d` |
| Parar | `Ctrl+C`, ou `docker compose down` |
| Ver os logs | `docker compose logs -f backend` |
| Aplicar mudança no `.env` | `docker compose restart backend` |
| Ver o estado dos containers | `docker compose ps` |
| **Apagar tudo e recomeçar** | `docker compose down -v` |
| Reconstruir após alterar código | `docker compose up --build` |

> ⚠️ `docker compose down -v` remove os volumes: **todos os dados enviados
> são apagados**, tanto do PostgreSQL quanto do ChromaDB. Use quando quiser
> um ambiente limpo.

---

## Estrutura do projeto

```
PortalTCC/
├── backend/                    API FastAPI
│   ├── main.py                 aplicação, rotas base e serviço do frontend
│   ├── config.py               configurações lidas do .env
│   ├── Dockerfile              imagem da aplicação
│   ├── routes/
│   │   ├── upload.py           POST /api/upload
│   │   └── chat.py             endpoints do chatbot
│   ├── services/
│   │   ├── csv_service.py      detecção de formato e tratamento dos CSVs
│   │   ├── db_service.py       persistência e consultas agregadas
│   │   ├── embedding_service.py geração de vetores e ChromaDB
│   │   └── chat_service.py     pipeline RAG
│   ├── db/connection.py        conexão e criação do schema
│   └── tests/                  testes automatizados
├── frontend/                   HTML, CSS e JS puro, sem build
│   ├── index.html              tela de upload
│   ├── chat.html               tela do chat
│   ├── css/
│   └── js/
├── Exemplos/                   CSV de exemplo para teste
├── docker-compose.yml          orquestração do banco e da aplicação
└── .env.example                modelo de configuração
```

O frontend é servido pelo próprio FastAPI, então não existe segundo
servidor nem build step — nada de Node, npm ou Live Server.

---

## Endpoints da API

Documentação interativa em **http://localhost:8000/docs** (Swagger) e
**http://localhost:8000/redoc**.

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/api/upload` | Envia um arquivo CSV |
| `GET` | `/api/table-info` | Estatísticas dos dados armazenados |
| `POST` | `/api/chat` | Pergunta com resposta completa |
| `POST` | `/api/chat/stream` | Pergunta com resposta em streaming |
| `DELETE` | `/api/chat/session/{id}` | Limpa o histórico de uma conversa |
| `GET` | `/api/info` | Informações da API |
| `GET` | `/health` | Verificação de disponibilidade |

---

## Solução de problemas

### "cannot connect to the Docker daemon"

O Docker não está rodando. No Windows e no macOS, abra o Docker Desktop e
espere ele indicar que está ativo. No Linux, rode `sudo systemctl start docker`.

### "port is already allocated" / a porta 8000 está ocupada

Outro programa já usa a porta 8000. Abra o `docker-compose.yml`, localize
no serviço `backend`:

```yaml
    ports:
      - "8000:8000"
```

Troque para:

```yaml
    ports:
      - "8080:8000"
```

E acesse http://localhost:8080.

### O chat responde que a chave não está configurada

É o comportamento esperado quando o `.env` está sem a
`GOOGLE_API_KEY`. Siga a seção [Gerando a chave da API do
Google](#gerando-a-chave-da-api-do-google).

### Preenchi a chave e o chat continua avisando que falta

Rode `docker compose restart backend` — a aplicação só lê o `.env` ao
iniciar.

Se ainda assim não funcionar, verifique se a variável não está exportada
no seu terminal, porque nesse caso ela tem prioridade sobre o arquivo:

```bash
echo $GOOGLE_API_KEY        # Linux, macOS, WSL
echo $env:GOOGLE_API_KEY    # Windows PowerShell
```

### O chat diz que não encontrou os dados

Confirme que o upload terminou com a mensagem de sucesso e que a chave
está configurada. Sem a chave, os vetores não são gerados no envio — nesse
caso, configure a chave e **envie o arquivo novamente**.

### "Apenas arquivos CSV são permitidos"

O sistema aceita apenas a extensão `.csv`. Se o seu arquivo é `.xlsx`,
abra-o no Excel ou LibreOffice e use *Salvar como → CSV*.

### O primeiro `docker compose up` parece travado

Não está travado. A primeira execução baixa cerca de 1 GB de imagens e
dependências. Espere alguns minutos; enquanto houver linhas novas
aparecendo no terminal, está progredindo.

### Quero apagar tudo e começar do zero

```bash
docker compose down -v
docker compose up --build
```

---

## Rodando sem Docker

Só recomendado para desenvolvimento. Exige **Python 3.12**, PostgreSQL e a
extensão TimescaleDB instalados na máquina.

```bash
# 1. Crie o banco
createdb portaltcc

# 2. Prepare o ambiente Python
cd backend
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 3. Configure
cp .env.example .env            # ajuste o DATABASE_URL para o seu banco

# 4. Rode
python main.py
```

O endereço de acesso continua sendo http://localhost:8000 — o FastAPI
serve o frontend também neste modo.

Para rodar os testes:

```bash
cd backend
python -m pytest tests/ -v
```

> **Python 3.12 é obrigatório.** Uma das dependências (`asyncpg 0.29.0`)
> não é compatível com a versão 3.13 ou superior.
