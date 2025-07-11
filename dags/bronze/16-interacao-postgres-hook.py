# ---
# ## DAG de Exemplo para Interação com PostgreSQL via `PostgresHook`
#
# Esta DAG demonstra como interagir com um banco de dados **PostgreSQL** utilizando o **`PostgresHook`**
# dentro de tarefas Python no Apache Airflow. Diferentemente do `PostgresOperator` (visto no arquivo `15-interacao-postgres.py`),
# que executa comandos SQL diretamente, o `PostgresHook` fornece uma interface Python para se conectar ao banco de dados
# e executar operações, oferecendo maior flexibilidade para lógica de programação mais complexa.
#
# Neste exemplo, cada etapa (criação de tabela, inserção de dados e consulta) é encapsulada em uma função Python separada,
# e cada função utiliza o `PostgresHook` para se conectar ao banco de dados e executar as operações SQL.
# O resultado da consulta é então passado via XComs para uma tarefa subsequente para impressão.
#
# **Atenção:** Para que este exemplo funcione corretamente, você precisará configurar uma **conexão PostgreSQL**
# chamada `"postgres"` na interface de usuário do Airflow (Admin -> Connections). Certifique-se de que as credenciais
# e o host do seu banco de dados PostgreSQL estejam configurados corretamente nesta conexão.
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
# ---

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # Importa o PostgresHook
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="16-interacao-postgres-hook",  # O ID único da sua DAG.
    description="Demonstra a interação com PostgreSQL usando PostgresHook em tarefas Python.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2023, 3, 5),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Função Python para criar uma tabela usando PostgresHook
def create_table_with_hook():
    # Instancia o PostgresHook, usando o ID da conexão configurada no Airflow.
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    print("Executando comando para criar tabela 'teste2'...")
    # Executa o comando SQL. 'autocommit=True' garante que a transação seja commitada automaticamente.
    pg_hook.run("CREATE TABLE IF NOT EXISTS teste2 (id INT);", autocommit=True)
    print("Tabela 'teste2' criada (se não existia).")

# Função Python para inserir dados usando PostgresHook
def insert_data_with_hook():
    # Instancia o PostgresHook.
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    print("Executando comando para inserir dados na tabela 'teste2'...")
    # Executa o comando SQL para inserir um registro.
    pg_hook.run("INSERT INTO teste2 VALUES (1);", autocommit=True)
    print("Dados inseridos na tabela 'teste2'.")

# Função Python para consultar dados usando PostgresHook e empurrar para XComs
def query_data_with_hook(**kwargs):
    ti = kwargs['ti'] # Acessa o TaskInstance para usar XComs.
    # Instancia o PostgresHook.
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    print("Executando consulta na tabela 'teste2'...")
    # Executa a consulta SQL e obtém todos os registros.
    records = pg_hook.get_records("SELECT * FROM teste2;")
    print(f"Registros encontrados: {records}")
    # Empurra os registros para o XCom com a chave 'query_result'.
    ti.xcom_push(key='query_result', value=records)
    print("Resultado da consulta empurrado para XCom.")

# Função Python para imprimir dados puxados do XCom
def print_data_from_xcom(**kwargs):
    ti = kwargs['ti'] # Acessa o TaskInstance.
    # Puxa o resultado da consulta do XCom, usando a chave e o task_id da tarefa que o gerou.
    task_instance_result = ti.xcom_pull(key='query_result', task_ids='select_data_task')
    print('--- Resultado da consulta puxado do XCom ---')
    if task_instance_result:
        for row in task_instance_result:
            print(row)
    else:
        print("Nenhum resultado puxado do XCom.")

# Define as tarefas Python que utilizam as funções com PostgresHook
create_table_task = PythonOperator(
    task_id='criar_tabela_hook',
    python_callable=create_table_with_hook,
    dag=dag
)

insert_data_task = PythonOperator(
    task_id='inserir_dados_hook',
    python_callable=insert_data_with_hook,
    dag=dag
)

select_data_task = PythonOperator(
    task_id='selecionar_dados_hook',
    python_callable=query_data_with_hook,
    provide_context=True, # Necessário para acessar 'ti' e XComs.
    dag=dag
)

print_data_task = PythonOperator(
    task_id='imprimir_dados_hook',
    python_callable=print_data_from_xcom,
    provide_context=True, # Necessário para acessar 'ti' e XComs.
    dag=dag
)

# Define a ordem de execução das tarefas
# As tarefas são executadas sequencialmente, garantindo que a tabela seja criada, dados inseridos,
# consultados e, finalmente, impressos.
create_table_task >> insert_data_task >> select_data_task >> print_data_task