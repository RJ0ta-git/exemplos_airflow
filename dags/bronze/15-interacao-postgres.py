# ---
# ## DAG de Exemplo para Interação com PostgreSQL (`PostgresOperator`)
#
# Esta DAG demonstra como interagir com um banco de dados **PostgreSQL** usando o **`PostgresOperator`** no Apache Airflow.
# Este operador é fundamental para tarefas de ETL/ELT que envolvem a execução de comandos SQL diretamente em um banco de dados PostgreSQL,
# como criação de tabelas, inserção de dados, consultas e outras operações de manipulação de dados.
#
# Neste exemplo, a DAG realizará as seguintes etapas:
# 1. Criará uma tabela simples (`teste`) se ela ainda não existir.
# 2. Inserirá um registro nesta tabela.
# 3. Executará uma consulta para selecionar todos os dados da tabela e empurrará o resultado para XComs.
# 4. Uma tarefa Python subsequente puxará esse resultado do XCom e o imprimirá.
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
# ---

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator # Importa o PostgresOperator
from datetime import datetime

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="15-interacao-postgres",  # O ID único da sua DAG.
    description="Demonstra a interação com um banco de dados PostgreSQL usando PostgresOperator.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2023, 3, 5),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define a função Python para imprimir o resultado da consulta
# Esta função recebe o TaskInstance (ti) via kwargs e puxa o resultado da consulta SQL do XCom.
def print_query_results(**kwargs):
    ti = kwargs['ti']
    # Puxa o resultado da tarefa 'query_data' do XCom.
    # O PostgresOperator, quando 'do_xcom_push=True', empurra o resultado da consulta para o XCom.
    query_result = ti.xcom_pull(task_ids='query_data')
    print('--- Resultado da consulta PostgreSQL ---')
    if query_result:
        for row in query_result:
            print(row)
    else:
        print("Nenhum resultado retornado da consulta.")

# Define a tarefa para criar a tabela
# Um PostgresOperator que executa um comando SQL para criar uma tabela se ela não existir.
# 'postgres_conn_id' deve corresponder ao ID da conexão configurada no Airflow.
create_table = PostgresOperator(
    task_id='criar_tabela_teste',
    postgres_conn_id='postgres',  # ID da conexão PostgreSQL.
    sql='CREATE TABLE IF NOT EXISTS teste (id INT);',  # Comando SQL para criar a tabela.
    dag=dag
)

# Define a tarefa para inserir dados na tabela
# Um PostgresOperator que executa um comando SQL para inserir um valor na tabela 'teste'.
insert_data = PostgresOperator(
    task_id='inserir_dados_teste',
    postgres_conn_id='postgres',  # ID da conexão PostgreSQL.
    sql='INSERT INTO teste VALUES (1);',  # Comando SQL para inserir dados.
    dag=dag
)

# Define a tarefa para consultar dados da tabela
# Um PostgresOperator que executa um comando SQL para selecionar todos os dados da tabela.
# 'do_xcom_push=True' é crucial aqui, pois faz com que o resultado da consulta seja empurrado para o XCom.
query_data = PostgresOperator(
    task_id='consultar_dados_teste',
    postgres_conn_id='postgres',  # ID da conexão PostgreSQL.
    sql='SELECT * FROM teste;',  # Comando SQL para consultar dados.
    do_xcom_push=True,  # Habilita o push do resultado da consulta para o XCom.
    dag=dag
)

# Define a tarefa para imprimir o resultado da consulta
# Um PythonOperator que executa a função 'print_query_results' para exibir os dados puxados do XCom.
print_query_results_task = PythonOperator(
    task_id='imprimir_resultado_consulta',
    python_callable=print_query_results,
    provide_context=True,  # Permite que a função acesse o contexto (incluindo 'ti' para XComs).
    dag=dag
)

# Define a ordem de execução das tarefas
# create_table >> insert_data: A inserção só ocorre após a criação da tabela.
# insert_data >> query_data: A consulta só ocorre após a inserção dos dados.
# query_data >> print_query_results_task: A impressão do resultado só ocorre após a consulta ser feita e o XCom ser preenchido.
create_table >> insert_data >> query_data >> print_query_results_task