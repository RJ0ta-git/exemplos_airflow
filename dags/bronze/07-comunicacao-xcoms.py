# ---
# ## DAG de Exemplo para Comunicação entre Tarefas (`XComs`)
#
# Esta DAG demonstra como as tarefas no Apache Airflow podem **compartilhar informações** entre si usando um mecanismo
# chamado **XComs** (Cross-communication). XComs são uma forma leve de comunicação entre tarefas, permitindo que uma tarefa
# "empurre" (`xcom_push`) um valor e outra tarefa "puxe" (`xcom_pull`) esse valor para uso posterior. Isso é extremamente útil
# para passar pequenos dados, como IDs de arquivos, resultados de processamento ou parâmetros de configuração, entre as etapas do seu pipeline.
#
# Neste exemplo, teremos uma tarefa que escreve um valor em um XCom e outra tarefa que lê e utiliza esse valor.
#
# Para mais detalhes sobre:
# - **Paralelismo básico**, consulte o arquivo `01-paralelismo-basico.py`.
# - **Encadeamento sequencial**, consulte o arquivo `02-encadeamento-sequencial.py`.
# - **Fluxo complexo com `trigger_rule`**, consulte o arquivo `03-fluxo-complexo-trigger-rule.py`.
# - **Organização com `TaskGroup`**, consulte o arquivo `04-organizacao-taskgroup.py`.
# - **Chamar outra DAG**, consulte o arquivo `05-chamar-outra-dag.py`.
# - **Configurações padrão e notificações por e-mail**, consulte o arquivo `06-notificacoes-email.py`.
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="07-comunicacao-xcoms",  # O ID único da sua DAG.
    description="Demonstra a comunicação entre tarefas usando XComs (xcom_push e xcom_pull).",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2021, 1, 1),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Função Python para a primeira tarefa que irá "escrever" um valor (push XCom)
# O argumento `**kwargs` é usado para acessar o TaskInstance (ti) e, por sua vez, os métodos xcom_push/xcom_pull.
def task_write_xcom(**kwargs):
    # Obtém o TaskInstance do contexto do Airflow.
    # O TaskInstance (ti) é o objeto que representa uma execução específica de uma tarefa em um DAG Run.
    ti = kwargs['ti']
    # Empurra um valor para o XCom.
    # 'key1' é a chave que será usada para recuperar o valor mais tarde.
    # 10200 é o valor que está sendo armazenado.
    ti.xcom_push(key='key1', value=10200)
    print("Valor '10200' empurrado para o XCom com a chave 'key1'.")

# Define a primeira tarefa: um PythonOperator que executa a função 'task_write_xcom'
task_push = PythonOperator(
    task_id="tarefa_escrever_xcom",  # ID da tarefa.
    python_callable=task_write_xcom,  # A função Python a ser executada.
    dag=dag
)

# Função Python para a segunda tarefa que irá "ler" um valor (pull XCom)
def task_read_xcom(**kwargs):
    # Obtém o TaskInstance do contexto do Airflow.
    ti = kwargs['ti']
    # Puxa o valor do XCom usando a chave 'key1'.
    # Por padrão, xcom_pull puxa o valor da tarefa diretamente upstream, mas pode-se especificar 'task_ids'.
    valor = ti.xcom_pull(key='key1')
    print(f'Valor lido do XCom (key1): {valor}')

# Define a segunda tarefa: um PythonOperator que executa a função 'task_read_xcom'
task_pull = PythonOperator(
    task_id="tarefa_ler_xcom",  # ID da tarefa.
    python_callable=task_read_xcom,  # A função Python a ser executada.
    dag=dag
)

# Define a dependência: 'task_read_xcom' só rodará após 'task_write_xcom' ser concluída com sucesso.
# Isso garante que o valor seja empurrado antes de ser puxado.
task_push >> task_pull