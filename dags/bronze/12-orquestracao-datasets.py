# ---
# ## DAG de Exemplo para Orquestração Orientada a Dados (`Datasets`)
#
# Esta DAG apresenta o conceito de **Datasets** no Apache Airflow, uma funcionalidade poderosa para
# **orquestração orientada a dados**. Em vez de agendar DAGs com base em tempo ou dependências diretas de tarefas,
# os Datasets permitem que DAGs sejam acionadas automaticamente quando um determinado conjunto de dados é "produzido"
# (ou seja, atualizado por outra tarefa ou DAG). Isso é ideal para cenários onde você tem fluxos de trabalho que
# dependem da disponibilidade ou atualização de dados específicos.
#
# Neste exemplo, uma tarefa Python lê um arquivo CSV e o salva novamente como outro arquivo, que é definido como um `Dataset`.
# Quando essa tarefa é concluída, ela "produz" o Dataset, o que pode, por sua vez, acionar outras DAGs que foram
# configuradas para "consumir" esse mesmo Dataset.
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
# - **Limpeza de dados com Pandas**, consulte o arquivo `11-limpeza-dados-pandas.py`.
# ---

from airflow import DAG
from airflow.datasets import Dataset  # Importa a classe Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="12-orquestracao-datasets",  # O ID único da sua DAG.
    description="Demonstra a orquestração orientada a dados com Airflow Datasets.",  # Uma breve descrição.
    schedule_interval=None,  # Esta DAG não tem um schedule_interval padrão; ela será acionada por um Dataset ou manualmente.
    start_date=datetime(2021, 1, 1),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define a função Python que irá processar o arquivo e "produzir" o Dataset
def my_file_processor():
    # Define o caminho do arquivo de entrada.
    input_path = '/opt/airflow/data/Churn.csv'
    # Define o caminho do arquivo de saída, que será o nosso Dataset.
    output_path = '/opt/airflow/data/Churn_Clean_new.csv'

    print(f"Lendo dados de: {input_path}")
    # Lê o arquivo CSV usando pandas.
    dataset = pd.read_csv(input_path, sep=';')

    print(f"Salvando dados processados em: {output_path}")
    # Salva o DataFrame em um novo arquivo CSV.
    # Esta ação de escrita no arquivo é o que o Airflow monitora para o Dataset.
    dataset.to_csv(output_path, sep=';', index=False)
    print("Processamento do arquivo concluído e Dataset atualizado.")

# Define o Dataset
# Um Dataset é criado com um URI (Uniform Resource Identifier), que é o caminho do arquivo ou recurso.
# O Airflow monitora este caminho para detectar quando o Dataset é atualizado.
my_dataset = Dataset('/opt/airflow/data/Churn_Clean_new.csv')

# Define a tarefa principal da DAG
# Um PythonOperator que executa a função 'my_file_processor'.
# O argumento 'outlets' é crucial aqui: ele indica que esta tarefa "produz" ou "atualiza" o 'my_dataset'.
# Quando esta tarefa é bem-sucedida, o Airflow registra que 'my_dataset' foi atualizado.
task1 = PythonOperator(
    task_id="processar_e_produzir_dataset",  # ID da tarefa.
    python_callable=my_file_processor,  # A função Python a ser executada.
    dag=dag,
    outlets=[my_dataset]  # Indica que esta tarefa produz o 'my_dataset'.
)

# Define a ordem de execução da DAG.
# Neste caso, há apenas uma tarefa de processamento de dados que produz o Dataset.
# Outras DAGs podem ser configuradas com 'schedule=my_dataset' para serem acionadas quando este Dataset for atualizado.
task1