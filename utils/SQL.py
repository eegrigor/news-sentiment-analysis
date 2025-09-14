import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import requests
import os 
import sys 
from dotenv import load_dotenv
import pandas as pd 
import ast 
import sqlite3

load_dotenv(dotenv_path="/opt/airflow/.env")

API_KEY = os.getenv("GNEWS_API_KEY")
COMPANIES = ast.literal_eval(os.getenv("COMPANIES"))
DB_PATH_VISUALIZATIONS = os.getenv("DB_PATH")

@st.cache_data(ttl=60)
def get_daily_data():
    """Load daily data for comopanies"""
    # Initialize connection to the database
    connection = sqlite3.connect(DB_PATH_VISUALIZATIONS)
    print(DB_PATH_VISUALIZATIONS)
    daily_df = pd.read_sql("SELECT * FROM daily_data", connection, parse_dates=["published_date"])

    # Close connection to databse 
    connection.close()

    return daily_df
