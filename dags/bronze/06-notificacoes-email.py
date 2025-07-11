# ---
# ## DAG de Exemplo para Configurações Padrão e Notificações por E-mail
#
# Esta DAG aprimora o entendimento sobre o uso de **`default_args`** no Apache Airflow, focando especificamente
# em como configurar **notificações por e-mail** em caso de falhas de tarefas. O `default_args` é um dicionário
# de configurações que podem ser aplicadas a todas as tarefas dentro de uma DAG, permitindo uma gestão centralizada
# de parâmetros importantes como o proprietário da DAG, a política de reexecução e, neste caso, os e-mails
# para onde as notificações serão enviadas.
#
# Neste exemplo, você verá como definir o e-mail do remetente e do destinatário nas configurações padrão,
# e como uma falha simulada (`task2` com `exit 1`) acionaria uma notificação por e-mail (assumindo que seu ambiente
# Airflow esteja configurado para enviar e-mails).
#
# Para mais detalhes sobre:
# - **Paralelismo básico**, consulte o arquivo `01-paralelismo-basico.py`.
# - **Encadeamento sequencial**, consulte o arquivo `02-encadeamento-sequencial.py`.
# - **Fluxo complexo com `trigger_rule`**, consulte o arquivo `03-fluxo-complexo-trigger-rule.py`.
# - **Organização com `TaskGroup`**, consulte o arquivo `04-organizacao-taskgroup.py`.
# - **Chamar outra DAG**, consulte o arquivo `05-chamar-outra-dag.py`.
# - **Configurações padrão e tratamento de falhas** (onde `retries` e `retry_delay` são explicados),
#   consulte o arquivo `06-default-args-falhas.py`.
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define os argumentos padrão para as tarefas dentro da DAG.
# Incluímos configurações para notificações por e-mail em caso de falha.
default_args = {
    'owner': 'airflow',  # O proprietário da tarefa.
    'depends_on_past': False,  # Não depende de execuções anteriores.
    'start_date': datetime(2025, 1, 1),  # A data de início padrão para as tarefas.
    'email': ['randolfo.junior.bi@gmail.com'],  # Lista de e-mails para notificação.
    'email_on_failure': True,  # Envia e-mail se a tarefa falhar.
    'email_on_retry': False,  # Não envia e-mail em cada tentativa de reexecução.
    'retries': 0,  # O número de vezes que uma tarefa tentará reexecutar em caso de falha.
    'retry_delay': timedelta(seconds=1)  # O atraso entre as tentativas de reexecução.
}

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="06-notificacoes-email",  # O ID único da sua DAG.
    default_args=default_args,  # Aplica os argumentos padrão definidos acima a todas as tarefas da DAG.
    schedule_interval='@daily',  # Define a frequência de execução da DAG.
    start_date=datetime(2025, 1, 1),  # Data de início da DAG.
    catchup=False,  # Não fará execuções retroativas.
    default_view='graph'  # Define a visualização padrão da DAG na UI do Airflow.
)

# Define a primeira tarefa
# Esta tarefa é uma BashOperator que simula um trabalho bem-sucedido.
task1 = BashOperator(
    task_id="tarefa_sucesso_1",
    bash_command="echo 'Executando Tarefa 1 com sucesso...'; sleep 2",
    dag=dag
)

# Define a segunda tarefa
# Esta tarefa é uma BashOperator que irá falhar intencionalmente ('exit 1').
# Devido a 'email_on_failure: True' nos default_args, um e-mail será enviado ao 'owner' configurado.
task2 = BashOperator(
    task_id="tarefa_com_falha_2",
    bash_command="echo 'Executando Tarefa 2 que irá falhar...'; exit 1",
    dag=dag
)

# Define a terceira tarefa
# Esta tarefa é uma BashOperator que simula outro trabalho bem-sucedido.
task3 = BashOperator(
    task_id="tarefa_sucesso_3",
    bash_command="echo 'Executando Tarefa 3 com sucesso...'; sleep 2",
    dag=dag
)

# Define as dependências entre as tarefas
# task1 >> [task2, task3]: task2 e task3 podem ser executadas em paralelo após task1 ser concluída.
task1 >> [task2, task3]