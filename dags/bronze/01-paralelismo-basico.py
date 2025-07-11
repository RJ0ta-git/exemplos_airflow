# Esta DAG demonstra como executar tarefas em paralelo no Apache Airflow. O paralelismo é fundamental para otimizar o tempo de execução de 
# fluxos de trabalho, permitindo que tarefas independentes sejam executadas simultaneamente. Neste exemplo, teremos duas tarefas simples que 
# rodam lado a lado, simulando operações independentes que não dependem uma da outra para iniciar.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
# Uma DAG é a coleção de todas as tarefas que você deseja executar, organizada de forma que reflita seus relacionamentos e dependências.
dag = DAG(
    dag_id="01-paralelismo-basico", # O ID único da sua DAG. Usamos o padrão "XX-nome-do-assunto".
    description="Demonstração de execução de tarefas em paralelo.", # Uma breve descrição do que a DAG faz.
    schedule_interval=None, # Define a frequência de execução da DAG. 'None' significa que ela será acionada manualmente ou via trigger externo.
    start_date=datetime(2021, 1, 1), # A data a partir da qual a DAG pode ser agendada para rodar.
    catchup=False # Se 'False', a DAG não fará execuções retroativas para agendamentos passados.
)

# Define a primeira tarefa
# Esta tarefa é uma BashOperator, que executa um comando bash.
task1 = BashOperator(
    task_id="task_paralela_1", # O ID único da tarefa dentro da DAG.
    bash_command="echo 'Executando tarefa paralela 1...'; sleep 2", # O comando bash a ser executado. 'sleep 2' simula um trabalho que leva 2 segundos.
    dag=dag # Associa esta tarefa à DAG que acabamos de criar.
)

# Define a segunda tarefa
# Similar à task1, esta é outra BashOperator, projetada para rodar em paralelo.
task2 = BashOperator(
    task_id="task_paralela_2", # O ID único da tarefa.
    bash_command="echo 'Executando tarefa paralela 2...'; sleep 2", # Outro comando bash que simula 2 segundos de trabalho.
    dag=dag # Associa esta tarefa à mesma DAG.
)

# Define a ordem de execução das tarefas
# Ao colocar as tarefas dentro de uma lista ([task1, task2]), estamos indicando que elas não têm dependência entre si
# e, portanto, podem ser executadas em paralelo pelo Airflow.
[task1, task2]