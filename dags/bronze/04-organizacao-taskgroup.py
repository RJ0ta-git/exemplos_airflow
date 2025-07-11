# ---
# ## DAG de Exemplo para Organização com `TaskGroup`
#
# Esta DAG apresenta o uso de **TaskGroup** no Apache Airflow, uma funcionalidade essencial para organizar visualmente
# e agrupar tarefas relacionadas dentro de um fluxo de trabalho complexo. TaskGroups ajudam a manter a clareza do
# grafo da DAG (Graph View), especialmente quando você tem muitas tarefas. Eles permitem encapsular um subconjunto
# de tarefas, tornando o DAG mais legível e gerenciável.
#
# Neste exemplo, veremos como as tarefas são definidas tanto fora quanto dentro de um `TaskGroup`, e como as dependências
# podem ser estabelecidas entre tarefas "externas" e o grupo.
#
# Para mais detalhes sobre:
# - **Paralelismo básico**, consulte o arquivo `01-paralelismo-basico.py`.
# - **Encadeamento sequencial**, consulte o arquivo `02-encadeamento-sequencial.py`.
# - **Fluxo complexo com `trigger_rule`**, consulte o arquivo `03-fluxo-complexo-trigger-rule.py`.
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
with DAG(
    dag_id="04-organizacao-taskgroup",  # O ID único da sua DAG.
    description="Demonstra o uso de TaskGroup para organizar tarefas em uma DAG.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2021, 1, 1),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
) as dag:
    # Define tarefas "externas" ao TaskGroup
    # Todas as tarefas são BashOperator que simulam um trabalho de 2 segundos.
    task1 = BashOperator(
        task_id="tarefa_1_externa",
        bash_command="echo 'Executando Tarefa 1 externa...'; sleep 2"
    )

    task2 = BashOperator(
        task_id="tarefa_2_externa",
        bash_command="echo 'Executando Tarefa 2 externa...'; sleep 2"
    )

    task3 = BashOperator(
        task_id="tarefa_3_externa",
        bash_command="echo 'Executando Tarefa 3 externa...'; sleep 2"
    )

    task4 = BashOperator(
        task_id="tarefa_4_externa",
        bash_command="echo 'Executando Tarefa 4 externa...'; sleep 2"
    )

    task5 = BashOperator(
        task_id="tarefa_5_externa",
        bash_command="echo 'Executando Tarefa 5 externa...'; sleep 2"
    )

    task6 = BashOperator(
        task_id="tarefa_6_externa",
        bash_command="echo 'Executando Tarefa 6 externa...'; sleep 2"
    )

    # Define um TaskGroup
    # Um TaskGroup é uma maneira de agrupar visualmente e logicamente tarefas relacionadas na interface do Airflow.
    # Ele ajuda a reduzir a complexidade visual de DAGs grandes.
    with TaskGroup(group_id="grupo_de_tarefas_internas") as taskgroup_interno:
        # Define tarefas dentro do TaskGroup
        # Essas tarefas pertencem logicamente ao 'grupo_de_tarefas_internas'.
        task7 = BashOperator(
            task_id="tarefa_7_interna",
            bash_command="echo 'Executando Tarefa 7 interna...'; sleep 2"
        )

        task8 = BashOperator(
            task_id="tarefa_8_interna",
            bash_command="echo 'Executando Tarefa 8 interna...'; sleep 2"
        )

        # Define a nona tarefa dentro do TaskGroup com uma trigger_rule específica
        # Esta tarefa usará 'one_failed' dentro do contexto do TaskGroup.
        task9 = BashOperator(
            task_id="tarefa_9_interna_trigger_one_failed",
            bash_command="echo 'Executando Tarefa 9 interna (one_failed)...'; sleep 2",
            trigger_rule='one_failed'
        )

    # Define as dependências entre as tarefas
    # task1 >> task2: task2 depende de task1.
    task1 >> task2
    # task3 >> task4: task4 depende de task3.
    task3 >> task4
    # [task2, task4] >> task5: task5 depende que task2 e task4 sejam concluídas.
    # task5 >> task6: task6 depende de task5.
    [task2, task4] >> task5 >> task6
    # task6 >> taskgroup_interno: O TaskGroup completo depende de task6.
    # Todas as tarefas dentro de 'taskgroup_interno' só iniciarão após task6 ser bem-sucedida.
    # As dependências internas ao TaskGroup (se houver) serão respeitadas após a conclusão de task6.
    task6 >> taskgroup_interno