import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import os
import time
from pathlib import Path

class IDALoader:
    """
    Classe responsável por carregar os dados transformados no Data Mart do Postgre
    """
    def __init__(self):
        """
        Inicializa a classe, cria o diretório de depuração e busca as credenciais
        do banco de dados a partir das variáveis de ambiente.
        """
        self.db_name = os.getenv("POSTGRES_DB")
        self.db_user = os.getenv("POSTGRES_USER")
        self.db_pass = os.getenv("POSTGRES_PASSWORD")
        self.db_host = "db"
        self.db_port = "5432"
        self.conn = None
        # Cria o diretório para os arquivos de depuração
        self.debug_dir = "data/debug"
        os.makedirs(self.debug_dir, exist_ok=True)

    def _connect(self):
        """
        Estabelece a conexão com o banco de dados, com retentativas.
        """
        retries = 5
        while retries > 0:
            try:
                if self.conn is None or self.conn.closed:
                    print("Conectando ao banco de dados PostgreSQL...")
                    self.conn = psycopg2.connect(
                        dbname=self.db_name,
                        user=self.db_user,
                        password=self.db_pass,
                        host=self.db_host,
                        port=self.db_port
                    )
                    print("Conexão bem-sucedida!")
                    return # Sai do loop se a conexão for bem-sucedida
            except psycopg2.OperationalError as e:
                print(f"Erro ao conectar ao banco de dados: {e}")
                retries -= 1
                if retries > 0:
                    print(f"Tentando novamente em 5 segundos... ({retries} tentativas restantes)")
                    time.sleep(5)
        
        print("Não foi possível conectar ao banco de dados após várias tentativas.")


    def _prepare_data(self, df, servico):
        """
        Prepara o DataFrame para a carga, adicionando a coluna de serviço
        e tratando a coluna de data.
        """
        print("Preparando dados para a carga...")
        df_copy = df.copy()

        # Adiciona a coluna de serviço que veio do loop do main.py
        df_copy['servico'] = servico

        # 1. converte a coluna 'Ano_Mes' para o formato de data.
        df_copy['mes_ano'] = pd.to_datetime(df_copy['Ano_Mes'], errors='coerce')

        # 2. Remove as linhas onde a conversão falhou.
        df_copy = df_copy.dropna(subset=['mes_ano'])
        
        # Salva o resultado da limpeza para análise.
        debug_filename = f"{servico}_depois_da_limpeza_final.csv"
        debug_path = os.path.join(self.debug_dir, debug_filename)
        print(f"Salvando arquivo de depuração em: {debug_path}")
        df_copy.to_csv(debug_path, index=False, encoding='utf-8-sig')

        # Se após a filtragem o DataFrame estiver vazio, retorna-o imediatamente.
        if df_copy.empty:
            print("Nenhum dado válido encontrado após a limpeza de datas. Pulando preparação.")
            return df_copy

        # Extrai ano e mês da coluna de data já validada
        df_copy['ano'] = df_copy['mes_ano'].dt.year
        df_copy['mes'] = df_copy['mes_ano'].dt.month

        # Limpa colunas desnecessárias
        df_copy = df_copy.drop(columns=['Ano_Mes'])
        
        # Renomeia colunas para corresponder ao banco de dados
        df_copy = df_copy.rename(columns={
            "GRUPO ECONÔMICO": "nome_grupo",
            "VARIÁVEL": "indicador",
            "Valor": "valor"
        })
        
        print("Amostra dos dados preparados antes da carga:")
        print(df_copy.head())

        return df_copy

    def _load_dimensions(self, cursor, df):
        """
        Carrega dados nas tabelas de dimensão (serviço, grupo, tempo) de forma idempotente
        e retorna mapeamentos de nome para ID para uso posterior.
        """
        dims = {}
        
        # Dimensão Serviço
        servicos = df[['servico']].drop_duplicates().values.tolist()
        execute_values(cursor, "INSERT INTO datamart_ida.dim_servico (nome_servico) VALUES %s ON CONFLICT (nome_servico) DO NOTHING", servicos)
        cursor.execute("SELECT id_servico, nome_servico FROM datamart_ida.dim_servico")
        dims['servico'] = {row[1]: row[0] for row in cursor.fetchall()}

        # Dimensão Grupo Econômico
        grupos = df[['nome_grupo']].drop_duplicates().values.tolist()
        execute_values(cursor, "INSERT INTO datamart_ida.dim_grupo_economico (nome_grupo) VALUES %s ON CONFLICT (nome_grupo) DO NOTHING", grupos)
        cursor.execute("SELECT id_grupo, nome_grupo FROM datamart_ida.dim_grupo_economico")
        dims['grupo'] = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Dimensão Tempo
        tempo = df[['ano', 'mes', 'mes_ano']].drop_duplicates().values.tolist()
        execute_values(cursor, "INSERT INTO datamart_ida.dim_tempo (ano, mes, mes_ano) VALUES %s ON CONFLICT (mes_ano) DO NOTHING", tempo)
        cursor.execute("SELECT id_tempo, mes_ano FROM datamart_ida.dim_tempo")
        dims['tempo'] = {row[1]: row[0] for row in cursor.fetchall()}
        
        print("Dimensões carregadas e mapeadas.")
        return dims

    def load_to_postgres(self, df, servico):
        """
        Orquestra o processo de carga: conecta, prepara os dados, carrega as dimensões
        e, finalmente, carrega a tabela fato.
        """
        self._connect()
        if self.conn is None:
            print("Não foi possível conectar ao banco. Abortando a carga.")
            return

        df_prepared = self._prepare_data(df, servico)

        # Se o DataFrame preparado estiver vazio, não há nada para carregar.
        if df_prepared.empty:
            return

        with self.conn.cursor() as cursor:
            try:
                # 1. Carregar Dimensões e obter mapeamentos de ID
                dims = self._load_dimensions(cursor, df_prepared)

                # 2. Mapear IDs no DataFrame principal para criar as colunas de chave estrangeira
                print("Mapeando IDs para a tabela fato...")
                df_prepared['id_servico'] = df_prepared['servico'].map(dims['servico'])
                df_prepared['id_grupo'] = df_prepared['nome_grupo'].map(dims['grupo'])
                df_prepared['id_tempo'] = df_prepared['mes_ano'].dt.date.map(dims['tempo'])
                
                # 3. Preparar e carregar Tabela Fato
                fato_cols = ['id_servico', 'id_grupo', 'id_tempo', 'indicador', 'valor']
                fato_data = df_prepared[fato_cols].values.tolist()
                
                print(f"Carregando {len(fato_data)} registros na tabela fato...")
                execute_values(cursor, "INSERT INTO datamart_ida.fato_ida (id_servico, id_grupo, id_tempo, indicador, valor) VALUES %s", fato_data)
                
                self.conn.commit()
                print("Carga concluída com sucesso para o serviço:", servico)

            except Exception as e:
                print(f"Erro durante a carga: {e}")
                self.conn.rollback()
            finally:
                if self.conn:
                    self.conn.close()
                    print("Conexão com o banco de dados fechada.")
