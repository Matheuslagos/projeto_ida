import os
import requests

class IDAExtractor:
    """
    Classe responsável por extrair os arquivos .ods da Anatel.
    Verifica se um arquivo já existe localmente antes de fazer o download.
    """
    def __init__(self, base_url, output_dir):
        self.base_url = base_url
        self.output_dir = output_dir
        # Garante que o diretório de saída exista
        os.makedirs(self.output_dir, exist_ok=True)

    def download(self, servico, ano):
        """
        Baixa um arquivo .ods para um determinado serviço e ano.
        Se o arquivo já existir na pasta de destino, o download é pulado.
        """
        # Define o nome e o caminho completo do arquivo de destino
        file_name = f"{servico}{ano}.ods"
        file_path = os.path.join(self.output_dir, file_name)

        # Verifica se o arquivo já existe no caminho de destino
        if os.path.exists(file_path):
            print(f"Arquivo '{file_name}' já existe. Usando a versão local em '{file_path}'.")
            return file_path
        # -------------------------

        # Se o arquivo não existir, prossegue com o download
        url = self.base_url.format(servico=servico, ano=ano)
        
        try:
            print(f"Baixando: {url}")
            response = requests.get(url)
            # Levanta um erro para status HTTP ruins
            response.raise_for_status()

            # Salva o conteúdo do arquivo baixado
            with open(file_path, "wb") as f:
                f.write(response.content)
            
            print(f"Arquivo salvo em {file_path}")
            return file_path

        except requests.exceptions.HTTPError as e:
            print(f"Erro ao baixar o arquivo {url}. Status: {e.response.status_code}")
            return None
