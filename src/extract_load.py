# Import Bibliotecas
import os
from urllib.parse import urlparse
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Carregar variaveis de ambiente do arquivo .env
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

# Import das Variaveis de Ambiente
commodities = [
    'GC=F',  # Ouro
    'SI=F',  # Prata
    'CL=F',  # Petroleo
]

def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

def buscar_todos_dados_commodities(lista_commodities):
    todos_dados = []
    for simbolo in lista_commodities:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

def salvar_dados_no_postgres(df, schema=None):
    destino_schema = schema or DB_SCHEMA
    df.to_sql('commodities', ENGINE, if_exists='replace', index=True, index_label='Data', schema=destino_schema)

if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities(commodities)
    salvar_dados_no_postgres(dados_concatenados)
    print(f"Os dados foram importados e salvos no banco de dados. {dados_concatenados.head()}")
