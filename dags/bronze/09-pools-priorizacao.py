# ---
# ## DAG de Exemplo para Gerenciamento de Concorrência com `Pools` e `priority_weight`
#
# Esta DAG demonstra como o Apache Airflow utiliza **`Pools`** para gerenciar a concorrência de tarefas e o
# **`priority_weight`** para influenciar a ordem de execução dentro de um pool. Pools são recursos limitados
# que você pode definir no Airflow para controlar o número máximo de tarefas que podem ser executadas simultaneamente
# em um determinado conjunto de recursos. Isso é crucial para evitar sobrecarga em sistemas externos (como bancos de dados ou APIs)
# ou para gerenciar a capacidade do seu próprio ambiente Airflow.
#
# Neste exemplo, todas as tarefas são atribuídas a um pool chamado `"meupool"`. Definiremos esse pool para ter uma
# capacidade limitada, o que forçará as tarefas a esperarem sua vez se o pool estiver cheio. Além disso,
# algumas tarefas têm um `priority_weight` maior, o que significa que, dentro do mesmo pool, elas terão preferência
# para serem executadas quando houver vagas.
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
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="09-pools-priority-weight",  # O ID único da sua DAG.
    description="Demonstra o uso de Pools para concorrência e priority_weight para priorização de tarefas.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2023, 3, 5),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# ATENÇÃO: Para este exemplo funcionar, você deve criar um Pool chamado "meupool" na interface de usuário do Airflow (Admin -> Pools).
# Defina o 'Slots' do "meupool" para um valor menor que 4 (por exemplo, 2) para observar o efeito da concorrência.

# Define a primeira tarefa
# Atribuída ao "meupool" com prioridade padrão.
task1 = BashOperator(
    task_id="tarefa_pool_1",
    bash_command="echo 'Executando Tarefa 1 (prioridade padrão)...'; sleep 5",
    dag=dag,
    pool="meupool"  # Atribui esta tarefa ao pool "meupool".
)

# Define a segunda tarefa
# Atribuída ao "meupool" com uma prioridade maior.
task2 = BashOperator(
    task_id="tarefa_pool_2_prioridade_alta_9",
    bash_command="echo 'Executando Tarefa 2 (prioridade 9)...'; sleep 5",
    dag=dag,
    pool="meupool",  # Atribui esta tarefa ao pool "meupool".
    priority_weight=9  # Define a prioridade desta tarefa. Tarefas com maior 'priority_weight' são preferidas.
)

# Define a terceira tarefa
# Atribuída ao "meupool" com prioridade padrão.
task3 = BashOperator(
    task_id="tarefa_pool_3",
    bash_command="echo 'Executando Tarefa 3 (prioridade padrão)...'; sleep 5",
    dag=dag,
    pool="meupool"  # Atribui esta tarefa ao pool "meupool".
)

# Define a quarta tarefa
# Atribuída ao "meupool" com a maior prioridade.
task4 = BashOperator(
    task_id="tarefa_pool_4_prioridade_alta_10",
    bash_command="echo 'Executando Tarefa 4 (prioridade 10)...'; sleep 5",
    dag=dag,
    pool="meupool",  # Atribui esta tarefa ao pool "meupool".
    priority_weight=10  # Define a prioridade mais alta.
)

# Define as dependências
# Todas as tarefas são independentes entre si, permitindo que o Airflow decida a ordem de execução
# com base na disponibilidade do pool e no 'priority_weight'.
[task1, task2, task3, task4]