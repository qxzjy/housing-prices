import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine

engine = create_engine(os.getenv("DB_URI"), echo=True)
table = os.getenv("TABLE")

def load_data() -> None:
    """
    Load data from database.
    """
    with engine.connect() as conn :
        database = pd.read_sql('SELECT * FROM housing_prices WHERE price IS NOT NULL ORDER BY id DESC', conn, index_col="id")

    return database

def write_db(data: pd.DataFrame) -> None:
    """
    Add a new house with his price predicted in the database.

    Args:
        data (pd.DataFrame): Dataframe containing the new house to add in the database.
    """
    try :
        data.to_sql(name=table, con=engine, index=False, if_exists="append")
        
    except Exception as e:
        print(e) 
