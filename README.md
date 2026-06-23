
# SteamGames-Data

**Projeto**: Pipeline de dados orquestrado com `Apache Airflow` que consome dados brutos do `MinIO`, segue a arquitetura medalhão (bronze/silver/gold) e materializa resultados finais em `PostgreSQL`, processando com `pandas` e `duckdb`.

**Visão Geral**
- **Descrição**: Este repositório implementa um pipeline onde os dados brutos são armazenados inicialmente no `MinIO` (camada bronze). O pipeline segue a arquitetura medalhão, aplicando transformações e enriquecimentos para gerar camadas silver e gold, e por fim carrega os resultados no `PostgreSQL` (destino final). Todo o processamento usa `pandas` e `duckdb`.
- **Orquestração**: As tarefas são definidas e agendadas com `Apache Airflow` (veja o DAG principal em `dags/top_games_2026.py`).

**Arquitetura**
- **Fonte**: `MinIO` — armazena os dados brutos iniciais (S3 compatível).
- **Armazenamento**: `MinIO` usado como Data Lake S3-compatível para as camadas Bronze/Silver/Gold, seguindo o padrão medalhão.
- **Processamento**: `pandas` para manipulação tabular e `duckdb` para consultas analíticas e junções em disco/memória.
- **Orquestração**: `Airflow` executando DAGs e tarefas que realizam extração, transformação e carregamento.

**Tecnologias e Bibliotecas**
- **Orquestração**: `Apache Airflow`
- **Data Lake**: `MinIO` (S3 compatível)
- **Processamento**: pandas, duckdb
- **Fonte de dados**: `MinIO` (dados brutos); `PostgreSQL` como destino final (inicialmente vazio)
- **Infra**: Docker / docker-compose (arquivo `docker-compose.yaml` no repositório)

**Estrutura do Repositório (resumo)**
- `dags/` : DAGs do `Airflow`; o DAG principal é `top_games_2026.py`.
- `src/` : Scripts de loaders e camadas (bronze, silver, gold).
- `config/` : Configurações do `Airflow`.
- `volumes/` : Volumes persistentes para `MinIO` e dados.
- `docker-compose.yaml` : Composição para levantar `Airflow` e `MinIO` localmente.

**Como executar (local)**
1. Ajuste as variáveis de conexão (Postgres, `MinIO`) conforme seu ambiente. Observe que o banco `PostgreSQL` começa vazio no projeto; o DAG popula o banco ao final do pipeline.
2. Inicie os serviços com Docker Compose:

```bash
docker-compose up -d
```

3. Acesse a interface do `Airflow` e acione o DAG `top_games_2026` ou aguarde o agendamento.

**Observações importantes**
- O DAG documenta o fluxo de cada etapa (leitura dos arquivos brutos no `MinIO`, processamento nas camadas bronze→silver→gold e carregamento final no `PostgreSQL` usando `duckdb`/`pandas`).
- Os scripts em `src/` contêm funções reutilizáveis para leitura/gravação no `MinIO` e transformações; o pipeline inclui tarefas que inserem as tabelas finais no `PostgreSQL` (que começa vazio neste projeto).

