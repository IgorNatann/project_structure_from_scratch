import os
import pandas as pd
import streamlit as st
from urllib.parse import urlparse
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

DB_SCHEMA = os.getenv('DB_SCHEMA_PROD', 'public')

def construir_database_url():
    """Monta a URL do banco suportando hosts completos ou apenas hostname."""
    host_raw = (os.getenv('DB_HOST_PROD') or '').strip()
    db_name = os.getenv('DB_NAME_PROD')
    db_port = os.getenv('DB_PORT_PROD')
    db_user = os.getenv('DB_USER_PROD')
    db_pass = os.getenv('DB_PASS_PROD')
    db_type = os.getenv('DB_TYPE_PROD', 'postgresql')
    db_driver = os.getenv('DB_DRIVER_PROD', 'psycopg2')

    if host_raw and '://' in host_raw:
        parsed = urlparse(host_raw)
        scheme = parsed.scheme or db_type
        username = parsed.username or db_user
        password = parsed.password or db_pass
        hostname = parsed.hostname
        port = parsed.port or db_port
    else:
        scheme = db_type
        username = db_user
        password = db_pass
        hostname = host_raw or None
        port = db_port

    drivername = scheme if '+' in scheme else f"{scheme}+{db_driver}" if db_driver else scheme

    return URL.create(
        drivername=drivername,
        username=username,
        password=password,
        host=hostname,
        port=int(port) if port else None,
        database=db_name,
    )

ENGINE = create_engine(construir_database_url())

def get_data():
    query = f"""
    SELECT
        data,
        simbolo,
        valor_fechamento,
        acao,
        quantidade,
        valor,
        ganho
    FROM
        public.dm_commodities;
    """
    df = pd.read_sql(query, ENGINE)
    return df

# Configurar a página do Streamlit
st.set_page_config(page_title='Dashboard do diretor', layout='wide')

# Título do Dashboard
st.title('Acompanhamento de Commodities')

# Descrição
st.write("""
Este dashboard mostra os dados de commodities e suas transações.
""")

# Obter os dados
df = get_data()

st.dataframe(df)
