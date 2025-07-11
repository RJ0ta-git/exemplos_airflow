# ---
# ## DAG de Exemplo para Limpeza de Dados com Pandas
#
# Esta DAG demonstra um pipeline simples de **limpeza e pré-processamento de dados** usando a biblioteca `pandas`
# em uma tarefa do Airflow. Este é um cenário comum em projetos de dados, onde os dados brutos precisam ser
# preparados e normalizados antes de serem usados para análise, modelagem ou carregamento em um data warehouse.
#
# Neste exemplo, a DAG lê um arquivo CSV de churn de clientes, realiza várias operações de limpeza
# (como preenchimento de valores ausentes, tratamento de outliers e remoção de duplicatas) e, em seguida,
# salva o conjunto de dados limpo em um novo arquivo CSV.
#
# Para mais detalhes sobre:
# - **Paralelismo básico**, consulte o arquivo `01-paralelismo-basico.py`.
# - **Encadeamento sequencial**, consulte o arquivo `02-encadeamento-sequencial.py`.
# - **Fluxo complexo com `trigger_rule`**, consulte o arquivo `03-fluxo-complexo-trigger-rule.py`.
# - **Organização com `TaskGroup`**, consulte o arquivo `04-organizacao-taskgroup.py`.
# - **Chamar outra DAG**, consulte o arquivo `05-chamar-outra-dag.py`.
# - **Configurações padrão e notificações por e-mail**, consulte o arquivo `06-notificacoes-email.py`.
# - **Comunicação entre tarefas (`XComs`)**, consulte o arquivo `07-comunicacao-xcoms.py`.
# - **Uso do `DummyOperator`**, consulte o arquivo `08-uso-dummy-operator.py`.
# - **Gerenciamento de concorrência com `Pools` e `priority_weight`**, consulte o arquivo `09-pools-priority-weight.py`.
# - **Ramificação condicional com `BranchPythonOperator`**, consulte o arquivo `10-ramificacao-condicional.py`.
# ---

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import statistics as sts # Importa a biblioteca statistics para calcular a mediana

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="11-limpeza-dados-pandas",  # O ID único da sua DAG.
    description="DAG para demonstrar limpeza e pré-processamento de dados com pandas.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2023, 3, 5),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define a função Python que realizará a limpeza dos dados
def clean_data():
    # Caminho para o arquivo CSV de entrada.
    # Certifique-se de que este caminho esteja acessível no ambiente do Airflow (ex: volume montado).
    input_file_path = '/opt/airflow/data/Churn.csv'
    # Caminho para o arquivo CSV de saída, onde os dados limpos serão salvos.
    output_file_path = '/opt/airflow/data/Churn_cleaned.csv'

    print(f"Lendo dados de: {input_file_path}")
    # Lê o arquivo CSV usando pandas. O separador é ';'.
    df = pd.read_csv(input_file_path, sep=';')

    # Renomeia as colunas para facilitar o acesso e padronização.
    df.columns = ["id", "Score", "Estado", "Genero", "Idade", "Patrimonio", "Saldo", "Produtos", "TemCardCredito", "Ativo", "Salario", "Saiu"]
    print("Colunas renomeadas.")

    # TRATAMENTO DE VALORES AUSENTES E OUTLIERS NO SALÁRIO
    # Calcula a mediana do salário para preencher valores ausentes.
    mediana_salario = sts.median(df["Salario"].dropna()) # Usar .dropna() para ignorar NaNs ao calcular a mediana
    # Preenche os valores ausentes (NaN) na coluna 'Salario' com a mediana calculada.
    df['Salario'].fillna(mediana_salario, inplace=True)
    print(f"Valores ausentes em 'Salario' preenchidos com a mediana: {mediana_salario}.")

    # TRATAMENTO DE VALORES AUSENTES NO GÊNERO
    # Preenche os valores ausentes (NaN) na coluna 'Genero' com a string 'Outro'.
    df['Genero'].fillna('Outro', inplace=True)
    print("Valores ausentes em 'Genero' preenchidos com 'Outro'.")

    # TRATAMENTO DE OUTLIERS NA IDADE
    # Calcula a mediana da idade para tratar valores fora de um range razoável.
    mediana_idade = sts.median(df["Idade"].dropna()) # Usar .dropna() para ignorar NaNs
    # Identifica idades inválidas (menores que 0 ou maiores que 120) e as substitui pela mediana.
    df.loc[(df['Idade'] < 0) | (df['Idade'] > 120), 'Idade'] = mediana_idade
    print(f"Outliers em 'Idade' tratados com a mediana: {mediana_idade}.")

    # Salva o DataFrame limpo em um novo arquivo CSV.
    # 'index=False' evita que o índice do DataFrame seja salvo como uma coluna no CSV.
    print(f"Salvando dados limpos em: {output_file_path}")
    df.to_csv(output_file_path, index=False, sep=';')
    print("Limpeza de dados concluída e arquivo salvo.")

# Define a tarefa principal da DAG
# Um PythonOperator que executa a função 'clean_data' definida acima.
task_clean_data = PythonOperator(
    task_id="limpeza_dados_churn",  # ID da tarefa.
    python_callable=clean_data,  # A função Python a ser executada.
    dag=dag
)

# Define a ordem de execução da DAG.
# Neste caso, há apenas uma tarefa, então ela será a única a ser executada.
task_clean_data