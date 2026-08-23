# Portal TCC

Portal web com chatbot RAG para análise de dados de espectrometria de
fluorescência de raios X portátil (pXRF).

O sistema recebe arquivos CSV gerados pelo equipamento, trata e persiste os
dados, indexa-os semanticamente e os torna consultáveis por um chatbot em
linguagem natural.

**Stack:** FastAPI · PostgreSQL + TimescaleDB · ChromaDB · LangChain · Google Gemini

---

## Como rodar

O único pré-requisito é o Docker. Não é necessário instalar Python,
PostgreSQL nem a extensão TimescaleDB — tudo roda em container.

```bash
git clone <url-do-repositorio>
cd PortalTCC
cp .env.example .env
docker compose up
```

Acesse **http://localhost:8000**

O primeiro `docker compose up` baixa as imagens e instala as dependências,
o que leva alguns minutos. As execuções seguintes sobem em segundos.

Para parar: `Ctrl+C`, ou `docker compose down` em outro terminal.

### Chave da API do Google

Edite o `.env` e preencha `GOOGLE_API_KEY` com uma chave gerada
gratuitamente em [aistudio.google.com/apikey](https://aistudio.google.com/apikey).

Você pode subir o sistema **sem a chave**: o envio de CSV e todo o
processamento de dados funcionam normalmente, e o chat responde com um
aviso explicando como configurá-la. Depois de preencher o `.env`, aplique
com:

```bash
docker compose restart backend
```

---

## Pré-requisitos por sistema

### Windows

Instale o [Docker Desktop](https://www.docker.com/products/docker-desktop/)
e mantenha-o aberto ao rodar os comandos. Durante a instalação, deixe a
opção **WSL 2** marcada.

Os comandos acima funcionam no PowerShell, com uma diferença: use
`copy .env.example .env` no lugar de `cp`.

### Linux

Instale o Docker Engine com o plugin Compose pelo
[guia oficial](https://docs.docker.com/engine/install/). Se preferir não
usar `sudo` em cada comando:

```bash
sudo usermod -aG docker $USER   # requer logout/login para valer
```

### macOS

Instale o [Docker Desktop](https://www.docker.com/products/docker-desktop/)
(há instaladores separados para Apple Silicon e Intel) e deixe-o aberto.

---

## Testando o sistema

O repositório inclui um arquivo de exemplo com dados reais de pXRF, em
`Exemplos/pXRF-Exemplo.csv` — 37 análises e 65 colunas de composição
química.

1. Abra http://localhost:8000
2. Envie o `Exemplos/pXRF-Exemplo.csv` pela tela de upload
3. Vá para a aba **Chat** e pergunte algo sobre os dados, por exemplo:
   - *Quais amostras estão presentes nos dados?*
   - *Qual é a média de Fe nas amostras?*
   - *Quantos registros foram carregados?*

O sistema aceita apenas arquivos `.csv`, de até 10 MB. Além do formato
pXRF, ele reconhece automaticamente CSVs dos tipos Nix e Visnir, e trata
CSVs genéricos.

---

## Estrutura

```
backend/          API FastAPI
  routes/         endpoints de upload e chat
  services/       tratamento de CSV, persistência, embeddings e RAG
  db/             conexão e criação do schema
frontend/         HTML/CSS/JS puro, servido pelo próprio FastAPI
Exemplos/         CSV de exemplo para teste
docker-compose.yml
```

A documentação interativa da API fica em http://localhost:8000/docs

---

## Problemas comuns

**A porta 8000 já está em uso.** Altere o mapeamento no
`docker-compose.yml`, no serviço `backend`, de `"8000:8000"` para
`"8080:8000"`, e acesse pela porta 8080.

**Editei o `.env` e a chave não mudou.** Rode `docker compose restart backend`.
Se persistir, verifique se a variável `GOOGLE_API_KEY` não está exportada
no seu shell — quando exportada, ela tem precedência sobre o `.env`.

**Quero começar do zero.** `docker compose down -v` remove os containers e
apaga os volumes com os dados do PostgreSQL e do ChromaDB.

**O chat responde que não encontrou os dados.** Confirme que o upload foi
concluído com sucesso e que a chave da API está configurada — sem ela, os
embeddings não são gerados e o chat não consegue consultar os registros.

---

## Desenvolvimento sem Docker

Ainda é possível rodar direto na máquina, com PostgreSQL e a extensão
TimescaleDB instalados localmente:

```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # ajuste o DATABASE_URL
python main.py
```

O frontend é servido pelo próprio FastAPI nos dois modos, então
http://localhost:8000 continua sendo o endereço de acesso.
