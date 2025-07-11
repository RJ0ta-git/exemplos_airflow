# ---
# ## DAG de Exemplo para Chamar Outra DAG (`TriggerDagRunOperator`)
#
# Esta DAG demonstra como uma DAG pode **acionar a execução de outra DAG** no Apache Airflow usando o operador `TriggerDagRunOperator`.
# Este é um padrão comum em arquiteturas de ETL/ELT e pipelines de dados, onde você pode ter uma DAG "mestra" que coordena a execução
# de várias DAGs "filhas", ou quando uma DAG de processamento precisa ser iniciada após a conclusão de uma DAG de ingestão de dados.
#
# O `TriggerDagRunOperator` permite que você especifique qual DAG deve ser acionada, permitindo a criação de fluxos de trabalho
# interdependentes e modularizados.
#
# Para mais detalhes sobre:
# - **Paralelismo básico**, consulte o arquivo `01-paralelismo-basico.py`.
# - **Encadeamento sequencial**, consulte o arquivo `02-encadeamento-sequencial.py`.
# - **Fluxo complexo com `trigger_rule`**, consulte o arquivo `03-fluxo-complexo-trigger-rule.py`.
# - **Organização com `TaskGroup`**, consulte o arquivo `04-organizacao-taskgroup.py`.
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="05-chamar-outra-dag",  # O ID único da sua DAG.
    description="Demonstra como uma DAG pode acionar a execução de outra DAG.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2021, 1, 1),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define a primeira tarefa
# Esta tarefa é uma BashOperator que simula um trabalho inicial.
task1 = BashOperator(
    task_id="tarefa_inicial",
    bash_command="echo 'Executando tarefa inicial...'; sleep 2",
    dag=dag
)

# Define a tarefa que acionará outra DAG
# O 'TriggerDagRunOperator' é usado para acionar programaticamente uma nova execução de DAG.
# 'trigger_dag_id' deve corresponder ao 'dag_id' da DAG que você deseja acionar.
# ATENÇÃO: Para este exemplo funcionar, você precisaria ter uma DAG com o ID "model_dag_1" (ou o ID da DAG que você deseja acionar)
# presente no seu ambiente Airflow.
task2 = TriggerDagRunOperator(
    task_id="acionar_dag_externa",  # O ID único desta tarefa.
    trigger_dag_id="01-paralelismo-basico",  # O ID da DAG que esta tarefa irá acionar. Substitua por um DAG ID existente.
    dag=dag
)

# Define a dependência: task2 (acionar_dag_externa) só rodará após task1 (tarefa_inicial) ser concluída.
task1 >> task2