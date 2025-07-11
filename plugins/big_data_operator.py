# plugins/big_data_operator.py

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults # apply_defaults é menos comum em versões recentes do Airflow (2.x), mas ainda funcional.
import pandas as pd

class BigDataOperator(BaseOperator):
    # apply_defaults é usado para preencher os argumentos padrão do operador automaticamente.
    # Em Airflow 2.x+, ele é frequentemente substituído por type hints e validação de construtor.
    @apply_defaults
    def __init__(self, path_to_csv_file, path_to_save_file, sep=';', file_type='parquet', *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.path_to_csv_file = path_to_csv_file
        self.path_to_save_file = path_to_save_file
        self.sep = sep
        self.file_type = file_type

    def execute(self, context):
        """
        Executa a lógica principal do operador.
        Lê um arquivo CSV e o salva em formato Parquet ou JSON.
        """
        self.log.info(f"Lendo dados do arquivo: {self.path_to_csv_file} com separador '{self.sep}'")
        try:
            df = pd.read_csv(self.path_to_csv_file, sep=self.sep)
            self.log.info(f"DataFrame lido com {len(df)} linhas e {len(df.columns)} colunas.")
        except FileNotFoundError:
            self.log.error(f"Erro: Arquivo não encontrado em {self.path_to_csv_file}")
            raise # Re-levanta a exceção para que a tarefa falhe

        self.log.info(f"Tentando salvar o arquivo em: {self.path_to_save_file} como tipo: '{self.file_type}'")
        if self.file_type == 'parquet':
            df.to_parquet(self.path_to_save_file, index=False) # index=False para não salvar o índice do pandas
            self.log.info("Arquivo salvo com sucesso em formato Parquet.")
        elif self.file_type == 'json':
            df.to_json(self.path_to_save_file, orient='records', lines=True) # orient='records', lines=True para um formato JSON mais comum para streaming
            self.log.info("Arquivo salvo com sucesso em formato JSON.")
        else:
            error_message = f"Tipo de dados inválido: '{self.file_type}'. Use 'parquet' ou 'json'."
            self.log.error(error_message)
            raise ValueError(error_message)