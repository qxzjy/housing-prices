import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine

engine = create_engine(os.getenv("DB_URI"), echo=True)
table = os.getenv("TABLE")

def load_data():

    with engine.connect() as conn :
        database = pd.read_sql('SELECT * FROM housing_prices WHERE price IS NOT NULL ORDER BY id DESC', conn, index_col="id")

    return database

def write_db(data):

    try :
        data.to_sql(name=table, con=engine, index=False, if_exists="append")
    except Exception as e:
        print(e) 
