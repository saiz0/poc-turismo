# Turismo Inteligente Bahia

PoC de uma busca turística híbrida que combina texto aproximado, filtros relacionais e similaridade semântica — executando localmente com Docker.

Em vez de exigir vários formulários, a aplicação recebe consultas em linguagem natural:

```text
trilha gratuita em Lençóis
parque pago em Lençóis e com acessibilidade
praias bonitas perto da capital
```

O sistema transforma requisitos explícitos em filtros seguros e usa embeddings para ordenar os resultados pelo significado da consulta.

> [!IMPORTANT]
> Os 5.000 locais de `dados_bahia.json` são sintéticos e servem exclusivamente para validar a arquitetura. Eles não representam informações turísticas reais.

## O que esta PoC demonstra

- Autocomplete por nome, cidade, categoria e descrição.
- Tolerância a palavras incompletas e erros de digitação.
- Busca semântica em português com embeddings de 1.024 dimensões.
- Extração de filtros por um LLM executado localmente.
- Validação determinística de cidade, categoria, preço e acessibilidade.
- PostgreSQL como banco relacional e vetorial na mesma arquitetura.
- Execução local sem depender de uma API de IA na nuvem.
- Importação assíncrona por Kafka, sem manter o endpoint HTTP bloqueado.
- Acompanhamento do progresso do job pela API e inspeção visual pelo Kafka UI.

## Arquitetura da busca

```text
                         ┌─────────────────────────┐
                         │ Consulta em português   │
                         └────────────┬────────────┘
                                      │
                        ┌─────────────▼─────────────┐
                        │ Ollama + parser de regras │
                        └────────┬───────────┬──────┘
                                 │           │
                     filtros duros           intenção semântica
                                 │           │
                    ┌────────────▼───┐   ┌───▼────────────────┐
                    │ PostgreSQL     │   │ multilingual-e5    │
                    │ cidade, tipo,  │   │ vetor de 1.024 dim.│
                    │ preço e acesso │   └───┬────────────────┘
                    └────────────┬───┘       │
                                 └─────┬─────┘
                               ┌───────▼────────┐
                               │ pgvector/HNSW │
                               │ ranking final │
                               └────────────────┘
```

O fluxo possui três partes:

1. **Busca lexical:** o `pg_trgm` sustenta o autocomplete e a correspondência aproximada.
2. **Filtros objetivos:** o Ollama propõe filtros, mas o parser valida valores e protege termos explicitamente escritos pelo usuário.
3. **Busca semântica:** o E5 gera o vetor da intenção e o `pgvector` ordena os registros filtrados por distância de cosseno.

## Arquitetura da importação

A geração dos embeddings não acontece dentro do processo HTTP:

```text
POST /popular
      │
      ├── cria import_job no PostgreSQL
      ├── publica { job_id } no Kafka
      └── responde HTTP 202
                    │
                    ▼
        turismo.importar-locais
                    │
                    ▼
              Coordenador
       lê o JSON e publica 5.000
          mensagens individuais
                    │
                    ▼
         turismo.processar-lugar
             (8 partições)
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
       Worker 1  Worker 2  Worker N
          └─────────┼─────────┘
                    ▼
          lugares_importacao
       (staging idempotente por
          job_id + lugar_id)
                    │
                    ▼
       último resultado finaliza o job
        e substitui a base ativa
```

A API continua disponível durante a carga. O coordenador realiza o fan-out, cada worker processa uma mensagem curta e o PostgreSQL mantém o estado do fan-in. Mensagens reentregues não duplicam lugares por causa da chave composta da tabela de staging.

### Por que combinar LLM e regras?

O LLM ajuda a interpretar linguagem subjetiva, mas não deve decidir sozinho restrições rígidas. O parser determinístico:

- normaliza caixa, acentos, pontuação e espaços;
- usa limites de palavra para evitar correspondências acidentais;
- reconhece aliases configuráveis, como `capital` → `Salvador`;
- dá precedência a negações, como `não acessível`;
- não escolhe arbitrariamente quando há conflito, como `gratuito ou pago`;
- descarta cidades e categorias inexistentes sugeridas pelo LLM;
- só aplica preço e acessibilidade quando esses requisitos aparecem explicitamente.

O vocabulário fica em [`config/query_rules.json`](config/query_rules.json), separado do código da API.

## Stack

| Tecnologia | Responsabilidade |
|---|---|
| FastAPI | API HTTP, validação de entrada e OpenAPI |
| PostgreSQL 16 | Dados turísticos e filtros relacionais |
| pg_trgm | Autocomplete e similaridade textual |
| pgvector + HNSW | Vetores, indexação e busca por cosseno |
| multilingual-e5-large | Embeddings multilíngues de 1.024 dimensões |
| Ollama + Llama 3.2 | Interpretação local das consultas |
| SQLAlchemy | Acesso ao banco e composição das consultas |
| HTML, CSS e JavaScript | Interface web sem framework |
| Apache Kafka | Fila durável para solicitar o processamento dos locais |
| Coordenador Python | Transforma uma solicitação em 5.000 mensagens individuais |
| Workers Python | Consomem lugares, geram embeddings e atualizam o staging |
| Kafka UI | Inspeção visual de tópicos, mensagens e consumer groups |
| Docker Compose | Orquestração local dos serviços |

## Início rápido

### Requisitos

- Docker Desktop ou Docker Engine com Compose v2.
- Pelo menos 8 GB de memória disponível são recomendados.
- Espaço em disco para as imagens Docker e os modelos Llama 3.2 e E5.

### 1. Configure o ambiente

No PowerShell:

```powershell
Copy-Item .env.example .env
```

No Linux ou macOS:

```bash
cp .env.example .env
```

Revise `POSTGRES_PASSWORD` antes de expor qualquer porta fora da máquina local.

### 2. Inicie os serviços

```bash
docker compose up --build -d
```

Confira a inicialização:

```bash
docker compose ps
docker compose logs -f api
```

### 3. Baixe o modelo do Ollama

```bash
docker compose exec ollama ollama pull llama3.2
```

Se `OLLAMA_MODEL` for alterado no `.env`, use o mesmo nome no comando acima.

### 4. Verifique a API

PowerShell:

```powershell
Invoke-RestMethod http://localhost:8000/health
```

Linux ou macOS:

```bash
curl http://localhost:8000/health
```

Resposta esperada:

```json
{"status":"ok"}
```

### 5. Enfileire a carga dos dados

PowerShell:

```powershell
Invoke-RestMethod -Method Post http://localhost:8000/popular
```

Linux ou macOS:

```bash
curl -X POST http://localhost:8000/popular
```

O endpoint responde imediatamente com HTTP `202` e um identificador:

```json
{
  "job_id": "3e6a36a0-...",
  "status": "pendente",
  "status_url": "/jobs/3e6a36a0-..."
}
```

O coordenador publica uma mensagem por lugar e os workers geram os embeddings. Consulte o progresso:

```powershell
$job = Invoke-RestMethod -Method Post http://localhost:8000/popular
Invoke-RestMethod "http://localhost:8000/jobs/$($job.job_id)"
```

> [!WARNING]
> Ao receber o último resultado, um worker substitui a base ativa dentro de uma transação. Apenas um job pode ficar ativo por vez. A primeira execução baixa o E5 em cada worker e pode levar vários minutos em CPU.

### 6. Abra a aplicação

- Interface: <http://localhost:8000>
- Swagger UI: <http://localhost:8000/docs>
- Health check: <http://localhost:8000/health>
- Kafka UI: <http://localhost:8080>

## Configuração

As variáveis disponíveis estão documentadas em [`.env.example`](.env.example):

| Variável | Valor em `.env.example` | Uso |
|---|---|---|
| `POSTGRES_USER` | `user` | Usuário do PostgreSQL |
| `POSTGRES_PASSWORD` | `troque-esta-senha` | Senha local do PostgreSQL |
| `POSTGRES_DB` | `turismo_db` | Nome do banco |
| `POSTGRES_PORT` | `5432` | Porta exposta pelo banco |
| `API_PORT` | `8000` | Porta exposta pela API |
| `OLLAMA_PORT` | `11434` | Porta exposta pelo Ollama |
| `OLLAMA_MODEL` | `llama3.2` | Modelo usado para interpretar consultas |
| `EMBEDDING_MODEL` | `intfloat/multilingual-e5-large` | Modelo de embeddings |
| `KAFKA_PORT` | `9092` | Porta publicada pelo broker para desenvolvimento |
| `KAFKA_UI_PORT` | `8080` | Porta exposta pela interface Kafka UI |
| `KAFKA_MAX_POLL_INTERVAL_MS` | `7200000` | Tempo máximo de um processamento antes do rebalanceamento |
| `KAFKA_SESSION_TIMEOUT_MS` | `45000` | Tempo de detecção de perda do consumer |
| `KAFKA_PLACE_PARTITIONS` | `8` | Partições do tópico de processamento; definido na criação do tópico |

O `.env` não deve ser enviado ao Git. Se as credenciais forem alteradas depois que o volume do PostgreSQL já foi criado, remova o volume e inicialize o banco novamente.

## API

| Método | Rota | Finalidade |
|---|---|---|
| `GET` | `/` | Interface web |
| `GET` | `/health` | Verifica a conexão com o PostgreSQL |
| `GET` | `/autocomplete?q=trilh` | Retorna até oito sugestões aproximadas |
| `GET` | `/buscar?q=...` | Executa a busca híbrida |
| `POST` | `/popular` | Cria um job e publica a solicitação no Kafka |
| `GET` | `/jobs` | Lista os jobs mais recentes |
| `GET` | `/jobs/{job_id}` | Retorna status, progresso e eventual erro |

### Estados de um job

| Estado | Significado |
|---|---|
| `pendente` | A solicitação aguarda o coordenador |
| `distribuindo` | O coordenador está publicando as mensagens dos lugares |
| `processando` | Os workers estão gerando embeddings individuais |
| `finalizando` | A base de staging está sendo promovida para a tabela ativa |
| `concluido` | Os 5.000 locais foram gravados e o índice foi analisado |
| `falhou` | O processamento terminou com erro; consulte o campo `erro` |

O endpoint retorna `409 Conflict` quando já existe uma importação pendente ou em processamento. Se a publicação no Kafka falhar, o job é marcado como `falhou` e a API responde `503 Service Unavailable`.

Exemplo de acompanhamento contínuo no PowerShell:

```powershell
$job = Invoke-RestMethod -Method Post http://localhost:8000/popular
do {
    $status = Invoke-RestMethod "http://localhost:8000/jobs/$($job.job_id)"
    $status | Select-Object status, processados, total, percentual, erro
    Start-Sleep -Seconds 5
} while ($status.status -in @('pendente', 'distribuindo', 'processando', 'finalizando'))
```

Exemplo de busca no PowerShell, com codificação segura da consulta:

```powershell
$consulta = [uri]::EscapeDataString('parque pago em Lençóis e com acessibilidade')
Invoke-RestMethod "http://localhost:8000/buscar?q=$consulta"
```

Exemplo com `curl`:

```bash
curl --get http://localhost:8000/buscar \
  --data-urlencode "q=praias bonitas perto da capital"
```

A resposta inclui os filtros interpretados e os resultados:

```json
{
  "filtros": {
    "cidade": "Salvador",
    "tipo": "Praia",
    "is_gratis": null,
    "tem_acessibilidade": null,
    "termo_semantico": "praias bonitas perto da capital"
  },
  "resultados": []
}
```

O array acima foi abreviado apenas para documentar o formato.

## Vocabulário e aliases

As regras externas estão divididas em três grupos:

```json
{
  "city_aliases": {},
  "type_aliases": {},
  "boolean_filters": {}
}
```

- `city_aliases`: mapeia expressões contextuais para cidades canônicas.
- `type_aliases`: associa categorias do banco aos seus sinônimos.
- `boolean_filters`: define expressões positivas e negativas para preço e acessibilidade.

Ao adicionar uma categoria, o nome canônico deve existir tanto nos dados quanto em `type_aliases`. Execute os testes após qualquer mudança no vocabulário.

## Dados sintéticos

O arquivo `dados_bahia.json` contém:

- 5.000 locais únicos;
- 20 cidades;
- 10 categorias;
- 250 registros por cidade;
- 500 registros por categoria.

Para recriá-lo deterministicamente:

```bash
python scripts/generate_data.py
```

Depois, reconstrua API e worker e execute novamente `POST /popular`:

```bash
docker compose build api coordinator worker
docker compose up -d api coordinator worker
```

A API e o coordenador precisam receber a mesma versão de `dados_bahia.json`: a API lê o total ao criar o job e o coordenador publica seu conteúdo no Kafka.

## Kafka UI

Acesse <http://localhost:8080> e selecione o cluster `turismo-local`.

As áreas mais úteis são:

- **Topics:** mostra `turismo.importar-locais`, `turismo.processar-lugar` e `turismo.processar-lugar.dlq`.
- **Consumers:** mostra `turismo-import-coordinator` e `turismo-place-workers` com seus offsets e lag.
- **Brokers:** apresenta o estado do broker local.

A Kafka UI é uma ferramenta administrativa de desenvolvimento. Ela não possui autenticação nesta PoC e não deve ser exposta publicamente.

### Onde as oito partições são definidas?

O serviço `kafka-init`, no `docker-compose.yml`, cria os tópicos antes do coordenador e dos workers iniciarem:

```yaml
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic turismo.processar-lugar \
  --partitions ${KAFKA_PLACE_PARTITIONS:-8} \
  --replication-factor 1
```

O valor configurável está no `.env.example`:

```env
KAFKA_PLACE_PARTITIONS=8
```

Os tópicos possuem configurações diferentes:

| Tópico | Partições | Motivo |
|---|---:|---|
| `turismo.importar-locais` | 1 | Uma solicitação deve ser expandida uma única vez pelo coordenador |
| `turismo.processar-lugar` | 8 | Permite distribuir lugares entre até oito workers ativos |
| `turismo.processar-lugar.dlq` | 8 | Mantém a origem paralela das mensagens que falharam |

O `kafka-init` termina com `Exited (0)` depois de criar ou verificar os tópicos. Esse estado significa sucesso; ele não é um serviço permanente.

Como a criação usa `--if-not-exists`, alterar o `.env` não modifica um tópico que já existe. Para aumentar manualmente o tópico de processamento:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic turismo.processar-lugar \
  --partitions 12
```

Kafka permite aumentar, mas não reduzir a quantidade de partições. Aumentar também altera a distribuição de novas mensagens por chave. Para um ambiente descartável, outra opção é executar `docker compose down -v`, alterar `KAFKA_PLACE_PARTITIONS` e recriar toda a infraestrutura — lembrando que isso apaga os dados persistidos.

### Relação entre partições e workers

Todos os workers usam o consumer group `turismo-place-workers`. Dentro desse grupo, uma partição pertence a no máximo um worker por vez:

```text
8 partições + 1 worker  → 8 partições para o worker
8 partições + 2 workers → aproximadamente 4 para cada
8 partições + 4 workers → aproximadamente 2 para cada
8 partições + 8 workers → 1 para cada
8 partições + 10 workers → 8 ativos e 2 ociosos
```

O coordenador não chama nem conhece os workers. Ele publica mensagens, e o Kafka distribui as partições entre os consumidores ativos.

## Desenvolvimento e validação

As verificações locais não exigem PostgreSQL, Ollama ou o download do E5:

```bash
python scripts/validate_project.py
python -m unittest discover -v
docker compose config --quiet
```

Elas validam:

- sintaxe dos arquivos Python;
- estrutura das regras externas;
- integridade e distribuição dos 5.000 registros;
- normalização, aliases, negações e conflitos do parser;
- validade do Docker Compose.

O workflow [`.github/workflows/validate.yml`](.github/workflows/validate.yml) executa as mesmas verificações em pushes e pull requests.

## Estrutura do projeto

```text
.
├── .github/workflows/validate.yml  # Integração contínua
├── config/query_rules.json         # Vocabulário e aliases do domínio
├── services/query_parser.py        # Parser determinístico
├── scripts/generate_data.py        # Gerador da base sintética
├── scripts/validate_project.py     # Validação sem infraestrutura
├── tests/test_query_parser.py      # Testes unitários do parser
├── app.py                          # API e busca híbrida
├── coordinator.py                  # Fan-out da importação em 5.000 mensagens
├── worker.py                       # Embedding individual, staging e fan-in
├── index.html                      # Interface web
├── dados_bahia.json                # 5.000 registros sintéticos
├── docker-compose.yml              # Infraestrutura, API, coordenador e workers
├── Dockerfile
├── LICENSE                          # Licença MIT
├── requirements.txt
└── .env.example
```

## Operação

```bash
# Ver serviços e healthchecks
docker compose ps

# Acompanhar a API
docker compose logs -f api

# Acompanhar o processamento assíncrono
docker compose logs -f coordinator
docker compose logs -f worker

# Escalar até o limite útil de oito partições
docker compose up -d --scale worker=4

# Listar jobs recentes
curl "http://localhost:8000/jobs?limite=10"

# Reiniciar somente o worker
docker compose restart worker

# Parar mantendo banco e modelos
docker compose down

# Apagar containers e volumes persistidos
docker compose down -v
```

O último comando apaga banco, modelo do Ollama e cache do E5. A próxima execução exigirá novos downloads e uma nova carga.

## Solução de problemas

### A API está saudável, mas o job continua pendente

Confira o coordenador, os workers e o Kafka:

```bash
docker compose ps
docker compose logs --tail 100 coordinator
docker compose logs --tail 100 worker
docker compose logs --tail 100 kafka
```

O coordenador deve consumir `turismo.importar-locais`. Os workers devem consumir `turismo.processar-lugar` no grupo `turismo-place-workers`.

### O job está processando, mas ainda mostra 0%

Na primeira execução, cada worker precisa baixar e carregar o modelo E5 antes de concluir seu primeiro lugar. O progresso começa a aumentar após a primeira mensagem gravada no staging.

### O log mostra `MAXPOLL` ou `ILLEGAL_GENERATION`

Um lugar demorou mais que `max.poll.interval.ms`, o Kafka removeu o consumer do grupo e rejeitou o commit. Como cada mensagem agora contém apenas um lugar, isso tende a ocorrer somente durante um download extremamente lento do modelo. A PoC mantém duas horas como margem para a primeira inicialização.

```bash
docker compose up --build -d worker
```

O staging usa `UNIQUE(job_id, lugar_id)`, portanto uma reentrega não incrementa o progresso nem duplica o lugar.

### A API retorna 409 ao criar uma carga

Já existe um job em estado ativo. Consulte `GET /jobs` e aguarde sua conclusão. Se algum processo foi interrompido permanentemente, investigue os dois consumer groups e seus lags no Kafka UI.

### As credenciais do PostgreSQL foram alteradas

As credenciais são aplicadas na criação inicial do volume. Em um ambiente descartável, recrie os volumes:

```bash
docker compose down -v
docker compose up --build -d
```

Esse procedimento apaga também os dados e caches persistidos pelo Compose.

### O build ocupa muito espaço

O `sentence-transformers` instala o PyTorch e suas dependências. Além disso, os modelos E5 e Llama ficam em volumes separados. Verifique o consumo com `docker system df`; não remova volumes se quiser preservar os modelos baixados.

## Limitações conhecidas

- Todos os locais são fictícios.
- `capital` significa Salvador porque o domínio desta PoC é a Bahia.
- Os aliases representam um vocabulário controlado, não um analisador completo da língua portuguesa.
- O ranking ainda não foi avaliado com usuários ou um conjunto de relevância rotulado.
- A carga substitui toda a base somente após gerar os embeddings; ainda não há atualização incremental.
- O modelo E5 Large exige memória e tem custo significativo de inicialização em CPU.
- O CORS está aberto para facilitar o desenvolvimento local.
- `/popular` não possui autenticação e não deve ser exposto publicamente dessa forma.
- A Kafka UI também não possui autenticação.
- A configuração Kafka usa um único broker, adequada para desenvolvimento e não para alta disponibilidade.
- Existe DLQ para mensagens de lugares com falha, mas ainda não há redrive automático.

## Licença

Este projeto é distribuído sob a [Licença MIT](LICENSE).
