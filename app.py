import pandas as pd
from sqlalchemy import create_engine
from faker import Faker
import random
from datetime import datetime, timedelta

# Inicializando o gerador de dados fictícios
fake = Faker()

# Conexão com o banco de dados SQL Server usando SQLAlchemy e pyodbc
DATABASE_URI = 'mssql+pyodbc://sa:123456@localhost:1433/desafioPentaho?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(DATABASE_URI)

# Função para gerar dados fictícios para a tabela Stage_Pacientes
def gerar_pacientes(n=10000):
    pacientes = []
    for _ in range(n):
        pacientes.append({
            'ID_Paciente': fake.unique.random_int(min=1, max=1000000),
            'Nome_Paciente': fake.name(),
            'Data_Nascimento': fake.date_of_birth(minimum_age=18, maximum_age=90),
            'Altura': f'{random.uniform(1.50, 2.00):.2f}',
            'Peso': f'{random.uniform(50.0, 100.0):.1f}',
            'Sexo': random.choice(['M', 'F']),
            'ID_Cliente': fake.random_int(min=1, max=100),
            'Data_Carga': datetime.now().date()
        })
    return pd.DataFrame(pacientes)

# Função para gerar dados fictícios para a tabela Stage_UnidadesSaude
def gerar_unidades_saude(n=10):
    unidades_saude = []
    for _ in range(n):
        unidades_saude.append({
            'ID_Unidade': fake.unique.random_int(min=1, max=1000000),
            'Nome_Unidade': fake.company(),
            'Localizacao': fake.address(),
            'Tipo_Unidade': random.choice(['Pública', 'Privada']),
            'Data_Carga': datetime.now().date()
        })
    return pd.DataFrame(unidades_saude)

# Função para gerar dados fictícios para a tabela Stage_Atendimentos
def gerar_atendimentos(n=100):
    atendimentos = []
    for _ in range(n):
        atendimentos.append({
            'ID_Atendimento': fake.unique.random_int(min=1, max=1000000),
            'ID_Paciente': fake.random_int(min=1, max=1000000),  # Deve estar alinhado com os IDs dos pacientes
            'ID_Unidade': fake.random_int(min=1, max=1000000),   # Deve estar alinhado com os IDs das unidades
            'Data_Atendimento': fake.date_between(start_date='-1y', end_date='today'),
            'Sintomas': ', '.join(fake.words(nb=random.randint(1, 4), ext_word_list=['Febre', 'Dor de cabeça', 'Tosse', 'Falta de ar', 'Dor abdominal'])),
            'Altura': f'{random.uniform(1.50, 2.00):.2f}',
            'Peso': f'{random.uniform(50.0, 100.0):.1f}',
            'Data_Carga': datetime.now().date()
        })
    return pd.DataFrame(atendimentos)

# Função para gerar dados fictícios para a tabela Stage_Sintomas
def gerar_sintomas():
    sintomas = [
        {'ID_Sintoma': 1, 'Descricao_Sintoma': 'Febre', 'Data_Carga': datetime.now().date()},
        {'ID_Sintoma': 2, 'Descricao_Sintoma': 'Dor de cabeça', 'Data_Carga': datetime.now().date()},
        {'ID_Sintoma': 3, 'Descricao_Sintoma': 'Tosse', 'Data_Carga': datetime.now().date()},
        {'ID_Sintoma': 4, 'Descricao_Sintoma': 'Falta de ar', 'Data_Carga': datetime.now().date()},
        {'ID_Sintoma': 5, 'Descricao_Sintoma': 'Dor abdominal', 'Data_Carga': datetime.now().date()},
    ]
    return pd.DataFrame(sintomas)

# Função para carregar os dados no banco de dados
def carregar_dados_no_bd(df, tabela_nome):
    try:
        df.to_sql(tabela_nome, engine, if_exists='replace', index=False)
        print(f'Dados carregados com sucesso na tabela {tabela_nome}!')
    except Exception as e:
        print(f'Erro ao carregar dados na tabela {tabela_nome}: {e}')

# Gerando e carregando os dados nas tabelas
pacientes_df = gerar_pacientes(100)  # Gerando 100 pacientes fictícios
unidades_saude_df = gerar_unidades_saude(10)  # Gerando 10 unidades de saúde fictícias
atendimentos_df = gerar_atendimentos(100)  # Gerando 100 atendimentos fictícios
sintomas_df = gerar_sintomas()  # Gerando a lista de sintomas fictícios

# Carregando os dados no banco de dados
carregar_dados_no_bd(pacientes_df, 'Stage_Pacientes')
carregar_dados_no_bd(unidades_saude_df, 'Stage_UnidadesSaude')
carregar_dados_no_bd(atendimentos_df, 'Stage_Atendimentos')
carregar_dados_no_bd(sintomas_df, 'Stage_Sintomas')
