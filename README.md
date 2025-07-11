# Repositório GitHub: Airflow - Exemplos de DAGs Comentadas em Português

Este repositório é uma coleção de 20 exemplos de Directed Acyclic Graphs (DAGs) para Apache Airflow, totalmente comentadas e explicadas em português. Nascido da percepção de que a maioria do conteúdo sobre Airflow está em inglês, este material visa preencher essa lacuna, oferecendo um caminho claro e estruturado para quem deseja aprofundar seus conhecimentos nesta ferramenta poderosa.

Os exemplos estão organizados em uma sequência lógica de estudo, começando por conceitos básicos e avançando gradualmente para tópicos mais complexos, como:
1.	Paralelismo e Encadeamento de Tarefas: Entenda como as tarefas podem ser executadas em paralelo ou sequencialmente.
2.	Condições de Acionamento (trigger_rule): Domine o controle de fluxo com regras como one_failed e all_failed.
3.	Organização de DAGs: Aprenda a estruturar DAGs complexas com DummyOperator e TaskGroup.
4.	Comunicação entre Tarefas (XComs): Troque dados entre tarefas de forma eficiente.
5.	Ramificação Condicional (BranchPythonOperator): Crie fluxos de trabalho dinâmicos baseados em condições.
6.	Manipulação de Dados com Pandas: Veja exemplos práticos de limpeza e transformação de dados.
7.	Orquestração Orientada a Dados (Datasets): Descubra como agendar DAGs com base na disponibilidade de dados.
8.	Monitoramento e Integrações: Utilize HttpSensor para verificar a disponibilidade de APIs e interaja com bancos de dados PostgreSQL usando PostgresOperator e PostgresHook.
9.	Desenvolvimento de Operadores Personalizados (Plugins): Entenda como estender o Airflow com suas próprias lógicas de negócio.

Pré-requisitos para Uso:
Para aproveitar ao máximo este repositório e executar as DAGs, você precisará de:

⦁	Docker e Docker Compose: O ambiente Airflow pode ser facilmente configurado usando o docker-compose.yaml incluído no repositório.

⦁	Conhecimento Básico de Python: As DAGs são escritas em Python, então uma familiaridade com a linguagem é essencial.

⦁	Ferramenta para Leitura de Código: Um IDE como VS Code ou PyCharm facilitará a exploração e compreensão dos exemplos.

⦁	Este material é ideal para estudantes, engenheiros de dados e qualquer profissional que deseje aprender ou aprimorar suas habilidades em Apache Airflow, com o diferencial de um conteúdo didático e totalmente em português.
