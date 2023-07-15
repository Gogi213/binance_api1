from sqlalchemy import create_engine
import pandas as pd

def connect_to_db():
    # Замените эти значения на свои
    db_name = "binance_project"
    db_user = "postgres"
    db_password = "19938713"
    db_host = "localhost"
    db_port = "5432"

    # Создайте строку подключения
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Создайте и верните движок SQLAlchemy
    engine = create_engine(connection_string)
    return engine

def update_table(conn, df, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
