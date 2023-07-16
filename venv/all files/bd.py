# bd.py
from sqlalchemy import create_engine
import pandas as pd

def connect_to_db(database='binance_project', user='postgres', password='19938713', host='localhost', port='5432'):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    return engine

def update_table(engine, df, table_name):
    df.to_sql(table_name, engine, if_exists='replace', index=False)