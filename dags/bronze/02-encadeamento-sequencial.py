# ---
# ## DAG de Exemplo para Encadear Tarefas (Ordem Sequencial)
#
# Esta DAG demonstra como **encadear tarefas** em uma **sequência específica** no Apache Airflow.
# Em muitos fluxos de trabalho, uma tarefa só pode começar depois que outra for concluída com sucesso.
# Este é o conceito de dependência de tarefas, e o Airflow oferece diferentes maneiras de definir essa ordem.
# Neste exemplo, vamos usar o método `set_upstream()` para garantir que as tarefas sejam executadas
# uma após a outra, em uma ordem decrescente de dependência (ou seja, `task3` deve terminar para `task2` começar,
# e `task2` deve terminar para `task1` começar).
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph) usando o gerenciador de contexto "with"
# O uso de "with DAG(...) as dag:" é uma prática recomendada, pois garante que o objeto DAG
# seja corretamente inicializado e que todas as tarefas definidas dentro do bloco "with"
# sejam automaticamente associadas a essa DAG, sem a necessidade de passar "dag=dag" em cada tarefa.
with DAG(
    dag_id="02-encadeamento-sequencial", # O ID único da sua DAG.
    description="Demonstração de encadeamento sequencial de tarefas usando set_upstream().", # Uma breve descrição do que a DAG faz.
    schedule_interval=None, # Define a frequência de execução da DAG. 'None' significa execução manual ou via trigger.
    start_date=datetime(2021, 1, 1), # A data a partir da qual a DAG pode ser agendada para rodar.
    catchup=False # Se 'False', a DAG não fará execuções retroativas para agendamentos passados.
) as dag:
    # Define a primeira tarefa
    # Esta tarefa é uma BashOperator que simula um trabalho de 2 segundos.
    task1 = BashOperator(
        task_id="primeira_tarefa", # O ID único da tarefa.
        bash_command="echo 'Executando a primeira tarefa...'; sleep 2" # O comando bash a ser executado.
    )

    # Define a segunda tarefa
    # Outra BashOperator, que também simula 2 segundos de trabalho.
    task2 = BashOperator(
        task_id="segunda_tarefa", # O ID único da tarefa.
        bash_command="echo 'Executando a segunda tarefa...'; sleep 2" # O comando bash a ser executado.
    )

    # Define a terceira tarefa
    # A última BashOperator, simulando 2 segundos de trabalho.
    task3 = BashOperator(
        task_id="terceira_tarefa", # O ID único da tarefa.
        bash_command="echo 'Executando a terceira tarefa...'; sleep 2" # O comando bash a ser executado.
    )

    # Define as dependências entre as tarefas
    # O método 'set_upstream()' estabelece que a tarefa à esquerda (task1) depende da tarefa à direita (task2).
    # Isso significa que 'task1' só será executada após 'task2' ser concluída com sucesso.
    task1.set_upstream(task2)

    # Da mesma forma, 'task2' só será executada após 'task3' ser concluída com sucesso.
    # A ordem de execução resultante será: task3 -> task2 -> task1.
    task2.set_upstream(task3)

    # Alternativamente, você poderia usar o operador de bitwise >> para definir a ordem, o que é mais comum e legível:
    # task3 >> task2 >> task1