# ---
# ## DAG de Exemplo para Monitoramento HTTP (`HttpSensor`)
#
# Esta DAG demonstra o uso do **`HttpSensor`** no Apache Airflow para monitorar a disponibilidade ou o estado de um endpoint HTTP.
# Sensores são tarefas especiais que esperam por uma condição específica ser verdadeira antes de permitir que as tarefas downstream
# sejam executadas. O `HttpSensor` é ideal para situações onde seu pipeline de dados depende da disponibilidade de um serviço web,
# API ou qualquer recurso acessível via HTTP.
#
# Neste exemplo, o `HttpSensor` (`check_api`) verificará um endpoint de API. Ele ficará "cutucando" (verificando repetidamente)
# o endpoint até que uma condição de sucesso seja atendida (por padrão, um código de status HTTP 200).
# Somente após o sensor ser bem-sucedido, a tarefa `process_data` (que simula uma consulta à API) será executada.
#
# **Atenção:** Para que este exemplo funcione corretamente, você precisará configurar uma **conexão HTTP** chamada `"conection_api"`
# na interface de usuário do Airflow (Admin -> Connections). A URL base da API (`https://www.omdbapi.com/`) deve ser definida
# nesta conexão. O `endpoint` no `HttpSensor` deve ser o caminho relativo após a URL base.
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
# ---

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime
import requests # Importa a biblioteca requests para fazer chamadas HTTP na função Python

# Define a DAG (Directed Acyclic Graph)
dag = DAG(
    dag_id="14-monitoramento-http-sensor",  # O ID único da sua DAG.
    description="Demonstra o uso do HttpSensor para aguardar a disponibilidade de um endpoint HTTP.",  # Uma breve descrição.
    schedule_interval=None,  # A DAG será acionada manualmente ou via trigger externo.
    start_date=datetime(2023, 3, 5),  # Data de início da DAG.
    catchup=False  # Não fará execuções retroativas.
)

# Define a função Python para consultar a API
# Esta função simula o processamento de dados após o sensor indicar que a API está disponível.
def query_api():
    # Faz uma requisição GET para a API OMDb.
    # Note que a chave 'apikey' é geralmente sensível e deveria ser gerenciada com Secret Backend no Airflow,
    # mas para fins de exemplo, está hardcoded aqui.
    print("Consultando a API OMDb...")
    response = requests.get('https://www.omdbapi.com/')
    # Imprime o texto da resposta da API.
    print(f"Resposta da API: {response.text}")
    response.raise_for_status() # Levanta um erro para códigos de status HTTP 4xx/5xx

# Define o HttpSensor
# Este sensor monitora um endpoint HTTP e só prossegue quando a condição é satisfeita.
check_api = HttpSensor(
    task_id='verificar_disponibilidade_api',  # ID da tarefa do sensor.
    http_conn_id='conection_api',  # O ID da conexão HTTP configurada no Airflow.
    endpoint='?i=tt3896198&apikey=e58b8373',  # O caminho relativo do endpoint da API.
    poke_interval=2,  # Intervalo em segundos entre as tentativas de "cutucar" (poke) o endpoint.
    timeout=20,  # Tempo máximo em segundos que o sensor esperará antes de falhar.
    dag=dag,
    # response_check=lambda response: "True" in response.text, # Exemplo de como verificar o conteúdo da resposta
    # tcp_check=True # Opcional: verifica a conectividade TCP antes de fazer a requisição HTTP completa
)

# Define a tarefa que processará os dados da API
# Este PythonOperator executa a função 'query_api' após o sensor ser bem-sucedido.
process_data = PythonOperator(
    task_id='processar_dados_api',
    python_callable=query_api,
    dag=dag
)

# Define a dependência: 'process_data' só será executada após 'check_api' ser bem-sucedido.
check_api >> process_data