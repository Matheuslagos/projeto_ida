from extractor import IDAExtractor
from transformer import IDATransformer
from loader import IDALoader
from view_manager import ViewManager

BASE_URL = "https://www.anatel.gov.br/dadosabertos/PDA/IDA/{servico}{ano}.ods"
OUTPUT_DIR = "data/raw"

anos = [2015]
servicos = ["SMP", "SCM", "STFC"]

if __name__ == "__main__":
    extractor = IDAExtractor(BASE_URL, OUTPUT_DIR)
    transformer = IDATransformer()
    
    # --- ETAPAS 1 e 2: Extração e Transformação ---
    for ano in anos:
        for servico in servicos:
            file_path = extractor.download(servico, ano)
            
            if file_path:
                df_long = transformer.wide_to_long(file_path)
                
                # --- ETAPA 3: Carga ---
                if not df_long.empty:
                    print(f"\n--- Iniciando carga para o serviço {servico} do ano {ano} ---")
                    # Criei uma nova instância do loader a cada iteração para garantir que a conexão seja nova e gerenciada corretamente.
                    loader = IDALoader()
                    loader.load_to_postgres(df_long, servico)
                else:
                    print(f"Nenhum dado para carregar para o serviço {servico} do ano {ano}.")
            else:
                print(f"Download falhou para {servico} do ano {ano}. Pulando.")

    # --- ETAPA 4: Criação da View ---
    print("\n--- Processo de ETL concluído. Iniciando criação da view. ---")
    # Ajuste o nome do arquivo aqui para corresponder ao seu novo nome
    view_manager = ViewManager("view/view_taxa_variacao.sql") 
    view_manager.execute_script()

