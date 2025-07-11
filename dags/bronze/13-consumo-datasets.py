# ---
# ## DAG de Exemplo para Consumo de Datasets (Orquestração Reativa)
#
# Esta DAG é um complemento direto da DAG `12-orquestracao-datasets.py` e demonstra como uma DAG pode ser
# **acionada automaticamente** quando um **Dataset** específico é atualizado. Em vez de um `schedule_interval`
# baseado em tempo, esta DAG usa o Dataset `mydataset` como seu acionador (`schedule=[mydataset]`). Isso significa que
# a DAG `13-consumo-datasets` só será executada quando a DAG `12-orquestracao-datasets` (ou qualquer outra tarefa
# que produza o `Churn_Clean_new.csv`) for concluída com sucesso e o arquivo `Churn_Clean_new.csv` for atualizado.
#
# Este padrão de consumo de Datasets é fundamental para criar pipelines de dados reativos, onde as etapas subsequentes
# são executadas somente quando os dados de entrada necessários estão prontos.
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
# - **Produção de Datasets**, consulte o arquivo `12-orquestracao-datasets.py`.
# ---

from airflow import DAG
from airflow.datasets import Dataset  # Importa a classe Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Define o Dataset que esta DAG irá "consumir".
# O Airflow monitorará este caminho. Se o arquivo for atualizado por outra tarefa (por exemplo, na DAG 12),
# esta DAG será agendada para execução.
my_dataset = Dataset('/opt/airflow/data/Churn_Clean_new.csv')

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="13-consumo-datasets",  # O ID único da sua DAG.
    description="DAG que é acionada por um Dataset (orquestração orientada a dados).",  # Uma breve descrição.
    schedule=[my_dataset],  # IMPORTANTE: A DAG é agendada para rodar quando 'my_dataset' for atualizado.
    start_date=datetime(2021, 1, 1),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define a função Python que irá processar o arquivo lido do Dataset
def my_file_consumer():
    # Define o caminho do arquivo de entrada, que é o Dataset que acionou esta DAG.
    input_path = '/opt/airflow/data/Churn_Clean_new.csv'
    # Define o caminho do arquivo de saída para esta DAG.
    output_path = '/opt/airflow/data/Churn_Clean_new_2.csv'

    print(f"Consumindo dados de: {input_path}")
    # Lê o arquivo CSV que foi atualizado.
    dataset = pd.read_csv(input_path, sep=";")

    print(f"Processando e salvando dados em: {output_path}")
    # Salva o DataFrame em um novo arquivo CSV, simulando um processamento adicional.
    dataset.to_csv(output_path, sep=";", index=False)
    print("Processamento do Dataset consumido concluído.")

# Define a tarefa principal da DAG
# Um PythonOperator que executa a função 'my_file_consumer'.
# 'provide_context=True' permite que a função acesse o contexto da execução da tarefa,
# embora não seja estritamente necessário para este exemplo de leitura simples.
task1 = PythonOperator(
    task_id="processar_dados_consumidos",  # ID da tarefa.
    python_callable=my_file_consumer,  # A função Python a ser executada.
    dag=dag,
    provide_context=True  # Permite acesso ao contexto do Airflow.
)

# Define a ordem de execução da DAG.
# Há apenas uma tarefa de processamento que consome o Dataset.
task1