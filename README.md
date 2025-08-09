
# Análise de Dados IDA - Anatel

Este projeto implementa um pipeline de ETL completo para extrair, transformar e carregar os dados do Índice de Desempenho no Atendimento (IDA) da Anatel em um Data Mart no PostgreSQL. Ao final, uma view analítica é criada para calcular a variação mensal dos indicadores.
---

### Índice

1.  [Visão Geral do Projeto](#1-visão-geral-do-projeto)
2.  [Tecnologias Utilizadas](#2-tecnologias-utilizadas)
3.  [Como Executar o Projeto](#3-como-executar-o-projeto)
4.  [Estrutura do Projeto](#4-estrutura-do-projeto)
5.  [Modelagem do Data Mart (Modelo Estrela)](#5-modelagem-do-data-mart-modelo-estrela)
6.  [Pipeline de ETL (Extração, Transformação e Carga)](#6-pipeline-de-etl-extração-transformação-e-carga)
7.  [View de Análise (`vw_variacao_ida`)](#7-view-de-análise-vw_variacao_ida)

---

## 1. Visão Geral do Projeto

O objetivo deste projeto é consolidar os dados do IDA da Anatel para os serviços de Telefonia Celular (SMP), Telefonia Fixa (STFC) e Banda Larga Fixa (SCM). Para isso, foi desenvolvido um pipeline de ETL automatizado que:
- **Extrai** os dados diretamente do portal de Dados Abertos.
- **Transforma** os dados de um formato largo (wide) para um formato longo (long), ideal para análise.
- **Carrega** os dados em um Data Mart com um modelo estrela no PostgreSQL.
- **Cria uma view** para análise da variação mensal dos indicadores.

Todo o ambiente é orquestrado com Docker e Docker Compose, garantindo a reprodutibilidade e facilidade de execução.

---

## 2. Tecnologias Utilizadas

- **Banco de Dados:** PostgreSQL `17.5-bookworm`
- **Linguagem de Programação:** Python `3.11.12-bookworm`
- **Bibliotecas Python:**
    - `pandas`: Para manipulação e transformação dos dados.
    - `requests`: Para realizar o download dos arquivos.
    - `psycopg2-binary`: Para a conexão com o PostgreSQL.
    - `odfpy`: Para a leitura de arquivos no formato `.ods`.
- **Conteinerização:** Docker e Docker Compose

---

## 3. Como Executar o Projeto

### Pré-requisitos

- Git
- Docker
- Docker Compose

### Passos para Execução

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/Matheuslagos/projeto_ida.git
    cd projeto_ida
    ```

2.  **Execute o Docker Compose:**
    Na pasta raiz do projeto (onde o arquivo `docker-compose.yml` está localizado), execute o seguinte comando:
    ```bash
    docker compose up
    ```
    Use sudo caso seja necessário
    - Este comando irá:
        - Iniciar o contêiner do PostgreSQL e criar o schema do Data Mart.
        - Construir a imagem Python.
        - Iniciar o contêiner Python, que executará todo o pipeline de ETL.
        - Ao final, a view analítica será criada no banco de dados.

3.  **Verifique o Resultado:**
    Após a execução ser concluída, você pode se conectar ao banco de dados PostgreSQL (usando DBeaver, pgAdmin ou outro cliente) com as seguintes credenciais:
    - **Host:** `localhost`
    - **Porta:** `5432`
    - **Banco:** `ida_db`
    - **Usuário:** `ida_user`
    - **Senha:** `ida_pass`

    Execute a seguinte consulta para ver o resultado final:
    ```sql
    SELECT * FROM datamart_ida.vw_variacao_ida;
    ```

---

## 4. Estrutura do Projeto

O projeto está organizado da seguinte forma para garantir a separação de responsabilidades:

<img width="241" height="478" alt="print_final" src="https://github.com/user-attachments/assets/7001b89f-eafe-4a7c-90cc-8fe727ca41f9" />

### Camadas de Dados

1.  **`data/raw/` (Dados Brutos):**
    - **Responsável:** `extractor.py`
    - **Conteúdo:** Contém os arquivos `.ods` originais, baixados diretamente da Anatel. Esta é a nossa fonte de dados bruta e imutável.

2.  **`data/transformed/` (Dados Transformados):**
    - **Responsável:** `transformer.py`
    - **Conteúdo:** Armazena uma versão em CSV dos dados após a primeira etapa de transformação (conversão de wide para long). Permite uma inspeção rápida da reestruturação dos dados.

3.  **`data/debug/` (Dados de Depuração):**
    - **Responsável:** `loader.py`
    - **Conteúdo:** Guarda uma versão final em CSV dos dados após a limpeza e preparação final (ex: tratamento de datas), pouco antes de serem carregados no banco. É a camada mais importante para depurar problemas de carga e de tipo de dados.


---

## 5. Modelagem do Data Mart (Modelo Estrela)

O Data Mart foi projetado usando um **Modelo Estrela** para otimizar as consultas analíticas. Este modelo consiste em uma tabela Fato central (`fato_ida`) cercada por tabelas de Dimensão (`dim_servico`, `dim_grupo_economico`, `dim_tempo`).

### Diagrama Entidade-Relacionamento (ER)

<img width="1439" height="678" alt="Screenshot from 2025-08-09 13-14-59" src="https://github.com/user-attachments/assets/dbf2d5e7-e57f-434f-a94d-5d75f4436bb8" />

### Descrição das Tabelas

- **`fato_ida` (Tabela Fato):** Armazena as métricas numéricas dos indicadores para cada combinação de serviço, grupo e tempo.
- **`dim_servico` (Dimensão):** Contém os nomes dos serviços de telecomunicação (SMP, SCM, STFC).
- **`dim_grupo_economico` (Dimensão):** Contém os nomes dos grupos econômicos (operadoras).
- **`dim_tempo` (Dimensão):** Contém as informações de data (ano, mês).

---

## 6. Pipeline de ETL (Extração, Transformação e Carga)

O processo de ETL é executado pelo script `main.py` e é dividido em classes com responsabilidades únicas:

1.  **Extração (`extractor.py`):** A classe `IDAExtractor` é responsável por baixar os arquivos `.ods` do portal de Dados Abertos da Anatel. Ela verifica se o arquivo já existe localmente para evitar downloads repetidos.

2.  **Transformação (`transformer.py`):** A classe `IDATransformer` recebe o caminho do arquivo baixado, lê os dados e os converte de um formato largo (com meses nas colunas) para um formato longo, mais adequado para análise.

3.  **Carga (`loader.py`):** A classe `IDALoader` conecta-se ao PostgreSQL, prepara os dados (tratando datas e renomeando colunas) e os carrega nas tabelas de dimensão e na tabela fato, garantindo a integridade referencial.

---

## 7. View de Análise (`vw_variacao_ida`)

Após a carga dos dados, o `view_manager.py` executa o script `view_taxa_variacao.sql` para criar a view final. Esta view calcula, para o indicador "Taxa de Resolvidas em 5 dias úteis":

- A taxa de variação média mensal, considerando todos os grupos.
- A diferença entre a taxa de variação individual de cada grupo e a média geral do mês.

###  Saída VIEW No pgadmin

<img width="1283" height="349" alt="printview" src="https://github.com/user-attachments/assets/0e537bed-498d-4722-9536-0d69012e7a96" />

