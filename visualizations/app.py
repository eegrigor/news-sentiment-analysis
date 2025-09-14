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
sys.path.insert(0, "/opt/airflow/utils") 
import SQL as sql
import altair as alt

load_dotenv(dotenv_path="/opt/airflow/.env")

COMPANIES = ast.literal_eval(os.getenv("COMPANIES"))

# Create page 
st.set_page_config(page_title="Company News Sentiment", layout="wide")
st.title("Company News Sentiment Dashboard")

# Create selection box to select wanted company
company = st.selectbox("Select a company", COMPANIES)
st.subheader(f"Daily Average Sentiment for {company}")

# Load data from the SQLite database 
daily_df = sql.get_daily_data()

# Get the daily data for the selected company 
company_daily = daily_df[daily_df['company'] == company]
company_daily = company_daily.sort_values("published_date")

# Plot the average sentiment for a the selected company
line_chart = (alt.Chart(company_daily).mark_line(point=True).encode(x=alt.X("published_date:T", title="Date"),
                                                         y=alt.Y("average_sentiment:N", title="Average Sentiment",
                                                                 scale=alt.Scale(domain=["POSITIVE", "NEUTRAL", "NEGATIVE"]),
                                                                 sort=None)).properties(height=500))
st.altair_chart(line_chart, use_container_width=True)

# Plot the ration of positive and negative aricles for each company 
st.subheader(f"Daily Sentiment Breakdown for {company}")
bar_chart = (alt.Chart(company_daily).transform_fold(["positive_count", "negative_count"],
                                                     as_=["Sentiment", "Count"]).mark_bar()
                                                     .encode(x=alt.X("published_date:T", title="Date"),
                                                             y=alt.Y("Count:Q", title="Article Count"),
                                                             color=alt.Color("Sentiment:N", title="Sentiment Type",
                                                                             scale=alt.Scale(domain=["positive_count", "negative_count"],
                                                                                             range=["green", "red"])),
                                                                                             tooltip=["published_date:T", "Sentiment:N", "Count:Q"])
                                                                                             .properties(height=500))
st.altair_chart(bar_chart, use_container_width=True)

