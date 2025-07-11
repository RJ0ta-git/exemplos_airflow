# ---
# ## DAG de Exemplo para Fluxo de Trabalho Complexo com `trigger_rule`
#
# Esta DAG demonstra um fluxo de trabalho mais complexo, combinando **paralelismo** e **encadeamento sequencial** de tarefas,
# e introduz o conceito de `trigger_rule`. O `trigger_rule` define as condições sob as quais uma tarefa pode ser executada,
# permitindo um controle mais granular sobre o fluxo de trabalho, especialmente em cenários de sucesso ou falha de tarefas upstream.
#
# Neste exemplo, temos várias tarefas que se ramificam e se unem, culminando em um conjunto final de tarefas
# onde uma delas (`task9`) utiliza a `trigger_rule='one_failed'`, o que significa que ela será executada
# se *qualquer uma* de suas tarefas upstream falhar.
#
# Para mais detalhes sobre:
# - **Paralelismo básico**, consulte o arquivo `01-paralelismo-basico.py`.
# - **Encadeamento sequencial**, consulte o arquivo `02-encadeamento-sequencial.py`.
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
# O uso de "with DAG(...) as dag:" é uma prática recomendada.
with DAG(
    dag_id="03-fluxo-complexo-trigger-rule", # O ID único da sua DAG.
    description="Demonstra um fluxo de trabalho complexo com paralelismo, encadeamento e trigger_rule.", # Uma breve descrição.
    schedule_interval=None, # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2021, 1, 1), # Data de início da DAG.
    catchup=False # Não fará execuções retroativas.
) as dag:
    # Define as tarefas
    # Todas as tarefas são BashOperator que simulam um trabalho de 2 segundos.
    task1 = BashOperator(
        task_id="tarefa_1",
        bash_command="echo 'Executando Tarefa 1...'; sleep 2"
    )

    task2 = BashOperator(
        task_id="tarefa_2",
        bash_command="echo 'Executando Tarefa 2...'; sleep 2"
    )

    task3 = BashOperator(
        task_id="tarefa_3",
        bash_command="echo 'Executando Tarefa 3...'; sleep 2"
    )

    task4 = BashOperator(
        task_id="tarefa_4",
        bash_command="echo 'Executando Tarefa 4...'; sleep 2"
    )

    task5 = BashOperator(
        task_id="tarefa_5",
        bash_command="echo 'Executando Tarefa 5...'; sleep 2"
    )

    task6 = BashOperator(
        task_id="tarefa_6",
        bash_command="echo 'Executando Tarefa 6...'; sleep 2"
    )

    task7 = BashOperator(
        task_id="tarefa_7",
        bash_command="echo 'Executando Tarefa 7...'; sleep 2"
    )

    task8 = BashOperator(
        task_id="tarefa_8",
        bash_command="echo 'Executando Tarefa 8...'; sleep 2"
    )

    # Define a nona tarefa com uma trigger_rule específica
    # 'trigger_rule='one_failed'' significa que esta tarefa será executada se pelo menos uma de suas tarefas upstream falhar.
    # As regras de trigger padrão são 'all_success' (todas as tarefas upstream devem ser bem-sucedidas).
    task9 = BashOperator(
        task_id="tarefa_9_trigger_one_failed",
        bash_command="echo 'Executando Tarefa 9 (one_failed)...'; sleep 2",
        trigger_rule='one_failed'
    )

    # Define as dependências entre as tarefas
    # task1 >> task2: task2 depende de task1.
    task1 >> task2
    # task3 >> task4: task4 depende de task3.
    task3 >> task4
    # [task2, task4] >> task5: task5 depende que task2 e task4 sejam concluídas (em paralelo).
    # task5 >> task6: task6 depende de task5.
    [task2, task4] >> task5 >> task6
    # task6 >> [task7, task8, task9]: task7, task8 e task9 dependem de task6 e podem rodar em paralelo.
    task6 >> [task7, task8, task9]