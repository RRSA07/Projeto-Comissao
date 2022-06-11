# Projeto-Comissao-Python
Projeto realizado para gerar e administrar as condições para o pagamento das comissões.

1- Extração dos dados

Foram utilizadas três fontes de dados para conseguir gerar uma tabela geral de vendas que unificasse as informações. As vendas do primeiro SaaS foram extraídas de um arquivo Excel, as do segundo SaaS foram obtidas de um formulário preenchido pelo vendedor armazenadas em uma planilha do Google Sheets enquanto que as vendas dos serviços atrelados aos treinamentos e configurações dos sistemas foram obtidos a partir de um banco de dados (PostgreSQL).

2- Geração das tabelas

Com os dados extraídos foi possível criar uma tabela de vendas geral e uma tabela de comissão no banco de dados (PostgreSQL). Para os percentuais das comissões de cada modalidade foi criado uma tabela também.

3- Metodologia

Os processos de extração, tratamento e armazenamento dos dados foram realizados no Python. Para realizar a interação com o banco de dados a biblioteca utilizada foi a Psycopg2, para conectar com o Google Sheets o Gspread e para os arquivos em Excel assim como para o tratamento e manipulação desses dados foram usadas as bibliotecas Pandas e Numpy.
