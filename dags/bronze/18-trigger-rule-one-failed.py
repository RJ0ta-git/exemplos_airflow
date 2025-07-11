# ---
# ## DAG de Exemplo para Condições de Acionamento (`trigger_rule`)
#
# Esta DAG demonstra o uso da propriedade **`trigger_rule`** em tarefas do Apache Airflow.
# A `trigger_rule` define as condições sob as quais uma tarefa deve ser executada, baseando-se
# no status das suas tarefas upstream (as tarefas que a precedem). Isso é crucial para construir
# fluxos de trabalho mais complexos e resilientes, onde você precisa de um controle preciso sobre a
# execução de tarefas dependendo do sucesso ou falha de outras.
#
# Neste exemplo, `task3` possui a `trigger_rule` definida como `'one_failed'`.
# Isso significa que `task3` será executada **se pelo menos uma de suas tarefas upstream
# (`task2` neste caso, ou `task1` se estivesse diretamente conectada) falhar**.
# Se `task2` for bem-sucedida, `task3` será ignorada (skipped).
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
# - **Consumo de Datasets**, consulte o arquivo `13-consumo-datasets.py`.
# - **Monitoramento HTTP com `HttpSensor`**, consulte o arquivo `14-monitoramento-http-sensor.py`.
# - **Interação com PostgreSQL via `PostgresOperator`**, consulte o arquivo `15-interacao-postgres.py`.
# - **Interação com PostgreSQL via `PostgresHook`**, consulte o arquivo `16-interacao-postgres-hook.py`.
# - **Uso de Operadores Personalizados (Plugins)**, consulte o arquivo `17-operador-personalizado-plugin.py`.
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator # Importa BashOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="18-trigger-rule-one-failed", # O ID único da sua DAG.
    description="Demonstra o uso da trigger_rule 'one_failed'.", # Uma breve descrição.
    schedule_interval=None, # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2021, 1, 1), # Data de início da DAG.
    catchup=False # Não fará execuções retroativas.
)

# Define a primeira tarefa
task1 = BashOperator(
    task_id="tarefa_inicial",
    bash_command="echo 'Executando Tarefa 1...'; sleep 2",
    dag=dag
)

# Define a segunda tarefa
# Esta tarefa pode ser alterada para 'exit 1' para simular uma falha e testar a trigger_rule de task3.
task2 = BashOperator(
    task_id="tarefa_intermediaria",
    bash_command="echo 'Executando Tarefa 2 (mude para exit 1 para testar falha)...'; sleep 2",
    # bash_command="echo 'Executando Tarefa 2 (irá falhar)...'; exit 1", # Descomente esta linha para testar a falha
    dag=dag
)

# Define a terceira tarefa com 'trigger_rule'
# Esta tarefa só será executada se uma das tarefas upstream (task2) falhar.
task3 = BashOperator(
    task_id="tarefa_em_caso_de_falha",
    bash_command="echo 'Tarefa 3 executada porque uma tarefa anterior falhou!'; sleep 2",
    dag=dag,
    trigger_rule='one_failed' # Esta é a chave: executa se qualquer upstream falhar.
)

# Define as dependências entre as tarefas
# task1 >> task2: task2 executa após task1.
# task2 >> task3: task3 executa após task2, mas sua trigger_rule 'one_failed' governa a condição.
task1 >> task2 >> task3