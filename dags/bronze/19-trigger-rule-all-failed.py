# ---
# ## DAG de Exemplo para Condições de Acionamento (`trigger_rule='all_failed'`)
#
# Esta DAG continua a demonstração das poderosas **`trigger_rule`** no Apache Airflow,
# focando especificamente na regra `'all_failed'`. Esta `trigger_rule` é usada quando você precisa
# que uma tarefa seja executada **somente se todas as suas tarefas upstream (predecessoras diretas) falharem**.
# Isso é útil para cenários de recuperação de falhas ou para acionar ações de notificação ou limpeza
# quando um conjunto crítico de tarefas não consegue ser concluído com sucesso.
#
# Neste exemplo, tanto `task1` quanto `task2` são configuradas para falhar (`bash_command="exit 1"`).
# A `task3` possui a `trigger_rule='all_failed'`, o que significa que ela só será executada se
# ambas `task1` e `task2` falharem. Se uma delas for bem-sucedida ou ignorada, `task3` também será ignorada.
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
# - **`trigger_rule='one_failed'`**, consulte o arquivo `18-trigger-rule-one-failed.py`.
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator  # Importa BashOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="19-trigger-rule-all-failed",  # O ID único da sua DAG.
    description="Demonstra o uso da trigger_rule 'all_failed'.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2021, 1, 1),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define a primeira tarefa que irá falhar
task1 = BashOperator(
    task_id="tarefa_falha_1",
    bash_command="echo 'Executando Tarefa 1 (irá falhar)...'; exit 1",  # Comando que força a falha da tarefa.
    dag=dag
)

# Define a segunda tarefa que também irá falhar
task2 = BashOperator(
    task_id="tarefa_falha_2",
    bash_command="echo 'Executando Tarefa 2 (irá falhar)...'; exit 1",  # Comando que força a falha da tarefa.
    dag=dag
)

# Define a terceira tarefa com 'trigger_rule'
# Esta tarefa só será executada se *todas* as suas tarefas upstream (task1 e task2) falharem.
task3 = BashOperator(
    task_id="tarefa_executa_se_tudo_falhar",
    bash_command="echo 'Tarefa 3 executada porque todas as tarefas anteriores falharam!'; sleep 2",
    dag=dag,
    trigger_rule='all_failed'  # Esta é a chave: executa SOMENTE se todos os upstreams falharem.
)

# Define as dependências entre as tarefas
# [task1, task2] >> task3: task3 tem duas tarefas upstream.
# A trigger_rule 'all_failed' fará com que task3 só seja agendada
# para execução se AMBAS task1 e task2 terminarem com status 'failed'.
[task1, task2] >> task3