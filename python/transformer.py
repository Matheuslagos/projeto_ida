import pandas as pd
import os
from pathlib import Path

class IDATransformer:
    def __init__(self):
        """
        Inicializa o transformer e cria o diretório de saída para
        os arquivos transformados, caso não exista.
        """
        self.output_dir = "data/transformed"
        os.makedirs(self.output_dir, exist_ok=True)

    def wide_to_long(self, file_path):
        """
        Lê um arquivo .ods, transforma os dados do formato wide para long,
        e salva uma cópia em CSV para inspeção.
        """
        print(f"Transformando {file_path}...")
        df = pd.read_excel(file_path, engine="odf", header=8)
        
        # Renomear colunas para padronizar
        df.columns = [str(col).strip() for col in df.columns]
        
        # Converte wide -> long
        df_long = df.melt(
            id_vars=["GRUPO ECONÔMICO", "VARIÁVEL"],
            var_name="Ano_Mes",
            value_name="Valor"
        )

        # Limpa dados
        df_long = df_long.dropna(subset=["Valor"])
        df_long["Ano_Mes"] = df_long["Ano_Mes"].astype(str)

        # --- NOVO TRECHO PARA SALVAR O ARQUIVO ---
        # Cria um nome de arquivo de saída baseado no nome do arquivo original
        original_filename = Path(file_path).stem
        output_filename = f"{original_filename}_transformed.csv"
        output_path = os.path.join(self.output_dir, output_filename)
        
        print(f"Salvando arquivo transformado para inspeção em: {output_path}")
        # Salva o DataFrame em formato CSV
        df_long.to_csv(output_path, index=False, encoding='utf-8-sig')
        # -----------------------------------------

        print(f"Transformação concluída ({len(df_long)} linhas)")
        return df_long
