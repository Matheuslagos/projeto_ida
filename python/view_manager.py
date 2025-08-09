import psycopg2
import os

class ViewManager:
    """
    Classe responsável por executar scripts SQL, como a criação de views.
    """
    def __init__(self, sql_script_path):
        """
        Inicializa a classe com o caminho para o script SQL.
        """
        self.db_name = os.getenv("POSTGRES_DB")
        self.db_user = os.getenv("POSTGRES_USER")
        self.db_pass = os.getenv("POSTGRES_PASSWORD")
        self.db_host = "db"
        self.db_port = "5432"
        self.sql_script_path = sql_script_path
        self.conn = None

    def _connect(self):
        """
        Estabelece a conexão com o banco de dados.
        """
        if self.conn is None or self.conn.closed:
            try:
                print("Conectando ao banco de dados para criar a view...")
                self.conn = psycopg2.connect(
                    dbname=self.db_name,
                    user=self.db_user,
                    password=self.db_pass,
                    host=self.db_host,
                    port=self.db_port
                )
            except psycopg2.OperationalError as e:
                print(f"Erro ao conectar: {e}")
                self.conn = None

    def execute_script(self):
        """
        Lê o arquivo SQL e o executa no banco de dados.
        """
        self._connect()
        if self.conn is None:
            print("Não foi possível conectar ao banco. Abortando criação da view.")
            return

        print(f"Lendo o script SQL da view de '{self.sql_script_path}'...")
        try:
            with open(self.sql_script_path, 'r') as f:
                sql_script = f.read()
        except FileNotFoundError:
            print(f"Erro: Arquivo SQL não encontrado em '{self.sql_script_path}'")
            return

        with self.conn.cursor() as cursor:
            try:
                print("Criando/Atualizando a view no banco de dados...")
                cursor.execute(sql_script)
                self.conn.commit()
                print("View criada/atualizada com sucesso!")
            except Exception as e:
                print(f"Erro ao executar o script da view: {e}")
                self.conn.rollback()
            finally:
                self.conn.close()
                print("Conexão para criação da view fechada.")
