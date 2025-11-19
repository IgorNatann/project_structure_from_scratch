# Import Bibliotecas
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Import das Variaveis de Ambiente
commodities = ['GC=F',  # Ouro
                'SI=F',  # Prata
                'CL=F',  # Petróleo
              ]

def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    ticker = yf.Ticker('CL=F')
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados


def buscar_todos_dados_commodities(commodities):
    todos_dados = []
    for ticker in commodities:
        dados = buscar_dados_commodities(ticker)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

# Efetuar a busca dos ativos

# Concatenar os dados

# Salvar os dados no Data Base

# Validação de execução
if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities(commodities)
    print(dados_concatenados)