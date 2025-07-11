# ---
# ## DAG de Exemplo para o Uso de `DummyOperator`
#
# Esta DAG demonstra a funcionalidade do **`DummyOperator`** no Apache Airflow. O `DummyOperator` é um operador "vazio",
# ou seja, ele não executa nenhuma ação real. Sua principal utilidade é puramente organizacional e estrutural,
# servindo como um ponto de junção ou ramificação em DAGs complexas para melhorar a clareza visual e lógica do seu pipeline.
# Ele é perfeito para agrupar visualmente tarefas ou para criar um ponto de sincronização onde várias tarefas upstream
# devem ser concluídas antes que as tarefas downstream possam começar.
#
# Neste exemplo, o `DummyOperator` `taskdummy` atua como um ponto de sincronização. Ele só será marcado como concluído
# depois que `task1`, `task2` e `task3` terminarem com sucesso, e somente então permitirá que `task4` e `task5` iniciem.
#
# Para mais detalhes sobre:
# - **Paralelismo básico**, consulte o arquivo `01-paralelismo-basico.py`.
# - **Encadeamento sequencial**, consulte o arquivo `02-encadeamento-sequencial.py`.
# - **Fluxo complexo com `trigger_rule`**, consulte o arquivo `03-fluxo-complexo-trigger-rule.py`.
# - **Organização com `TaskGroup`**, consulte o arquivo `04-organizacao-taskgroup.py`.
# - **Chamar outra DAG**, consulte o arquivo `05-chamar-outra-dag.py`.
# - **Configurações padrão e notificações por e-mail**, consulte o arquivo `06-notificacoes-email.py`.
# - **Comunicação entre tarefas (`XComs`)**, consulte o arquivo `07-comunicacao-xcoms.py`.
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator # Importa o DummyOperator (em versões mais recentes do Airflow, é airflow.operators.dummy)

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="08-uso-dummy-operator",  # O ID único da sua DAG.
    description="Demonstra o uso do DummyOperator para organização e sincronização de tarefas.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2023, 3, 5),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define as tarefas iniciais
# São BashOperators simples que simulam um pequeno atraso.
task1 = BashOperator(task_id="tarefa_inicial_1", bash_command="echo 'Executando tarefa 1...'; sleep 1", dag=dag)
task2 = BashOperator(task_id="tarefa_inicial_2", bash_command="echo 'Executando tarefa 2...'; sleep 1", dag=dag)
task3 = BashOperator(task_id="tarefa_inicial_3", bash_command="echo 'Executando tarefa 3...'; sleep 1", dag=dag)

# Define o DummyOperator
# Este operador não faz nada. Ele simplesmente espera que todas as suas tarefas upstream sejam concluídas.
taskdummy = DummyOperator(task_id="dummy_sincronizador", dag=dag)

# Define as tarefas finais
# Essas tarefas só iniciarão após o DummyOperator ser concluído.
task4 = BashOperator(task_id="tarefa_final_1", bash_command="echo 'Executando tarefa 4...'; sleep 1", dag=dag)
task5 = BashOperator(task_id="tarefa_final_2", bash_command="echo 'Executando tarefa 5...'; sleep 1", dag=dag)

# Define as dependências
# [task1, task2, task3] >> taskdummy: Todas as tarefas task1, task2 e task3 devem ser concluídas
# antes que o taskdummy possa ser executado.
# taskdummy >> [task4, task5]: Após o taskdummy ser concluído, task4 e task5 podem ser executadas em paralelo.
[task1, task2, task3] >> taskdummy >> [task4, task5]