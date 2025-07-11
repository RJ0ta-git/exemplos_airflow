# ---
# ## DAG de Exemplo para Uso de Operadores Personalizados (Plugins)
#
# Esta DAG demonstra como o Apache Airflow permite estender sua funcionalidade através de **plugins**,
# que podem incluir operadores, hooks, sensores e interfaces personalizados. O uso de operadores personalizados,
# como o `BigDataOperator` neste exemplo, é fundamental para encapsular lógicas de negócios específicas e reutilizáveis,
# tornando suas DAGs mais limpas, legíveis e fáceis de manter.
#
# Neste caso, o `BigDataOperator` simula uma operação de processamento de dados que lê um arquivo CSV,
# potencialmente realiza alguma transformação (implícita pelo nome do operador), e salva o resultado em um formato diferente (JSON).
#
# **Atenção:** Para que este exemplo funcione, você precisaria ter o **`BigDataOperator` definido como um plugin personalizado**
# em seu ambiente Airflow. Este operador não faz parte da instalação padrão do Airflow. Ele seria implementado em
# um arquivo Python dentro da pasta `plugins` do seu diretório Airflow.
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
# ---

from airflow import DAG
# A linha abaixo pressupõe que 'big_data_operator' está acessível no PYTHONPATH do Airflow,
# tipicamente porque foi definido como um plugin.
# from big_data_operator import BigDataOperator # Importa o operador personalizado
from datetime import datetime

# Para o propósito deste exemplo, vamos simular a classe BigDataOperator
# para que o arquivo possa ser executado sem a necessidade de um plugin real.
# Em um ambiente Airflow real, você substituiria esta classe pela importação do seu plugin.
class BigDataOperator(object):
    def __init__(self, task_id, path_to_csv_file, path_to_save_file, sep, file_type, dag):
        self.task_id = task_id
        self.path_to_csv_file = path_to_csv_file
        self.path_to_save_file = path_to_save_file
        self.sep = sep
        self.file_type = file_type
        self.dag = dag
        print(f"BigDataOperator '{task_id}' inicializado.")
        print(f"  Lendo de: {path_to_csv_file}")
        print(f"  Salvando em: {path_to_save_file} como {file_type}")

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="17-operador-personalizado-plugin",  # O ID único da sua DAG.
    description="Modelo de DAG demonstrando o uso de um operador personalizado (plugin).",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2021, 1, 1),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Instancia o BigDataOperator
# Este operador personalizado recebe vários parâmetros para configurar sua operação de dados.
big_data_operator_task = BigDataOperator(
    task_id="processar_dados_grandes",  # ID da tarefa.
    path_to_csv_file="/opt/airflow/data/Churn.csv",  # Caminho do arquivo CSV de entrada.
    path_to_save_file="/opt/airflow/data/Churn_alterado.json",  # Caminho para salvar o arquivo de saída.
    sep=";",  # Separador do arquivo CSV de entrada.
    file_type="json",  # Tipo de arquivo para o resultado (ex: json).
    dag=dag
)

# Define a ordem de execução da DAG.
# Neste caso, há apenas uma tarefa que utiliza o operador personalizado.
big_data_operator_task