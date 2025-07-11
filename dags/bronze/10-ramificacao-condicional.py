# ---
# ## DAG de Exemplo para Ramificação Condicional (`BranchPythonOperator`)
#
# Esta DAG demonstra como implementar **ramificações condicionais** em seus fluxos de trabalho do Apache Airflow
# usando o **`BranchPythonOperator`**. A ramificação condicional permite que você execute diferentes caminhos de
# tarefas com base em uma lógica de negócios específica, tornando suas DAGs mais dinâmicas e adaptáveis.
#
# Neste exemplo, geramos um número aleatório. Em seguida, uma tarefa de ramificação avalia se esse número é par ou ímpar
# e, com base nessa condição, direciona o fluxo da DAG para uma tarefa específica (`par_task` se for par,
# ou `impar_task` se for ímpar).
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
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator # Importa o BranchPythonOperator
from datetime import datetime
import random

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="10-ramificacao-condicional",  # O ID único da sua DAG.
    description="Demonstra ramificação condicional com BranchPythonOperator.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2023, 3, 5),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define a função Python para gerar um número aleatório
# Esta função será usada pelo PythonOperator para gerar um valor.
def gerar_numero_aleatorio():
    # Retorna um número inteiro aleatório entre 1 e 100.
    return random.randint(1, 100)

# Define a tarefa que gera o número aleatório
# Um PythonOperator que executa a função 'gerar_numero_aleatorio'.
# O valor retornado pela função será automaticamente empurrado para o XCom.
gera_num_aleatorio = PythonOperator(
    task_id="gerar_numero_aleatorio",  # ID da tarefa.
    python_callable=gerar_numero_aleatorio,  # A função Python a ser executada.
    dag=dag
)

# Define a função Python para avaliar o número e decidir o caminho
# Esta função será usada pelo BranchPythonOperator. Ela puxa o XCom da tarefa anterior.
def avaliar_numero(**context):
    # Puxa o número gerado pela tarefa 'gerar_numero_aleatorio' do XCom.
    # 'task_instance' (ti) é passado via 'context' e permite interagir com XComs.
    numero = context['ti'].xcom_pull(task_ids="gerar_numero_aleatorio")
    print(f"Número gerado: {numero}")
    # Verifica se o número é par ou ímpar e retorna o ID da tarefa correspondente.
    if numero % 2 == 0:
        print("Número é par. Prosseguindo para 'par_task'.")
        return "par_task"  # Retorna o task_id da tarefa a ser executada.
    else:
        print("Número é ímpar. Prosseguindo para 'impar_task'.")
        return "impar_task"  # Retorna o task_id da tarefa a ser executada.

# Define a tarefa de ramificação
# O BranchPythonOperator executa a função 'avaliar_numero' e, com base no seu retorno,
# permite que apenas a tarefa com o 'task_id' correspondente seja executada.
branch_task = BranchPythonOperator(
    task_id="ramificacao_par_impar",  # ID da tarefa de ramificação.
    python_callable=avaliar_numero,  # A função Python que decide o caminho.
    provide_context=True,  # Necessário para que a função possa acessar o 'context' (e o 'ti').
    dag=dag
)

# Define a tarefa para quando o número for par
# Uma BashOperator que simplesmente imprime uma mensagem.
par_task = BashOperator(
    task_id="par_task",  # Este task_id deve corresponder ao retorno da função 'avaliar_numero'.
    bash_command='echo "O número gerado é par!"',
    dag=dag
)

# Define a tarefa para quando o número for ímpar
# Uma BashOperator que simplesmente imprime uma mensagem.
impar_task = BashOperator(
    task_id="impar_task",  # Este task_id deve corresponder ao retorno da função 'avaliar_numero'.
    bash_command='echo "O número gerado é ímpar!"',
    dag=dag
)

# Define as dependências
# gera_num_aleatorio >> branch_task: A tarefa de ramificação só pode ser executada após o número ser gerado.
# branch_task >> par_task: O Airflow irá "pular" esta tarefa se a função 'avaliar_numero' retornar "impar_task".
# branch_task >> impar_task: O Airflow irá "pular" esta tarefa se a função 'avaliar_numero' retornar "par_task".
gera_num_aleatorio >> branch_task
branch_task >> par_task
branch_task >> impar_task