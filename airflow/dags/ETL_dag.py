import requests
import os 
import sys 
from dotenv import load_dotenv
import pandas as pd 
import ast 
sys.path.insert(0, "/opt/airflow/utils") 
import NLP as nlp
import sqlite3
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task

load_dotenv(dotenv_path="/opt/airflow/.env")

API_KEY = os.getenv("GNEWS_API_KEY")
COMPANIES = ast.literal_eval(os.getenv("COMPANIES"))
DB_PATH = os.getenv("DB_PATH")
TMP_DIR = "/opt/airflow/dags/tmp"

default_args = {"owner": "admin",                 
                "retries": 1,                        
                "retry_delay": timedelta(minutes=1)}

with DAG(dag_id="company_news_sentiment_pipeline",
         start_date=days_ago(2),                   
         schedule_interval="@daily",                
         catchup=True,                             
         default_args=default_args,                 
         max_active_runs=1,                         
         tags=["companies", "news", "nlp"],        
) as dag:
    
    @task()
    def fetch_news():
        """Fetch news for each of the selected companies from API."""
        
        articles = []
        # Request company articles for each company 
        for company in COMPANIES:
            url = f"https://gnews.io/api/v4/search?q={company}&lang=en&max=40&apikey={API_KEY}"
            data = requests.get(url=url).json()
            # Add each artcile for each company to the array articles
            for article in data.get("articles", []):
                articles.append({"company": company,
                                "title": article.get("title"),
                                "description": article.get("description"),
                                "content": article.get("content"),
                                "published_at": article.get("publishedAt"),
                                "source": article.get("source", {}).get("name"),
                                "url": article.get("url")
                                })
            
        # Save raw news data in a csv file
        df_articles = pd.DataFrame(articles)
        raw_path = f"{TMP_DIR}/raw_data.csv"
        df_articles.to_csv(raw_path, index=False)
        
        return raw_path


    @task(multiple_outputs=True)
    def transform(raw_data_path: str):
        """Receives as input the raw data path and transforms the data."""

        raw_data_df = pd.read_csv(raw_data_path)

        # Drop rows with NaN in description column
        raw_data_df = raw_data_df.dropna(subset=["description"])

        raw_data_df["sentiment"] = raw_data_df["description"].apply(lambda x : nlp.sentiment_classification(x))

        # Trim only the date that the article was published 
        raw_data_df["published_date"] = pd.to_datetime(raw_data_df["published_at"]).dt.date

        # Compute daily average sentiment for each company
        daily_sentiment = raw_data_df.groupby(["company", "published_date"]).agg(average_sentiment=("sentiment", lambda x : x.mode()[0]),
                                                                                positive_count=("sentiment", lambda x: (x=="POSITIVE").sum()),
                                                                                negative_count=("sentiment", lambda x: (x=="NEGATIVE").sum()))
        
        # Check for equal count of positive and negative atricles and set average_sentiment to NEUTRAL 
        mask = (daily_sentiment["positive_count"] == daily_sentiment["negative_count"])
        daily_sentiment.loc[mask, "average_sentiment"] = "NEUTRAL"

        transformed_data_path = f"{TMP_DIR}/transformed_data.csv"
        daily_sentiment_data_path = f"{TMP_DIR}/daily_data.csv"

        # Save the dataframes 
        raw_data_df.to_csv(transformed_data_path)
        daily_sentiment.to_csv(daily_sentiment_data_path)

        return {"transformed_data_path": transformed_data_path, "daily_sentiment_data_path": daily_sentiment_data_path}
    
    @task()
    def load_data(transformed_data_path: str, daily_sentiment_data_path: str):
        """Loads data into a SQLite database"""
        
        # Connect to databse 
        connection = sqlite3.connect(DB_PATH)

        # Load data from csv fiels
        transformed_data_df = pd.read_csv(transformed_data_path)
        daily_data_df = pd.read_csv(daily_sentiment_data_path)

        # Load data into the database
        transformed_data_df.to_sql("transformed_data", connection, if_exists='append', index=False)
        daily_data_df.to_sql("daily_data", connection, if_exists='append', index=False)

        # Close connection with the database 
        connection.close()
        
        return True
    
    raw_data_path = fetch_news()
    paths = transform(raw_data_path)
    load_data(paths["transformed_data_path"], paths["daily_sentiment_data_path"])