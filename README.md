# Company News Sentiment Analysis

This project uses an **Airflow DAG**that fetches news articles from the [GNews API](https://gnews.io/)
, analyzes their sentiment using **NLP**, and stores the results in a **SQLite**  database. Additionally, it uses **Streamlit** to visualize sentiment trends over the past days and provide a breakdown of the number of positive and negative articles.

## Environment Variables 

In the `.env` file you can find some environment variables used in this project: 

- *GNEWS_API_KEY* : Your API key for accessing GNews API
. Required to fetch news articles.

- *COMPANIES* : A list of company names for which you want to fetch news.
- *DB_PATH* : Path to the SQLite database. 
- *AIRFLOW__WEBSERVER__SECRET_KEY* : Secret key for Airflow webserver session security.
- *AIRFLOW__CORE__FERNET_KEY* : Key used by Airflow to encrypt sensitive information in the metadata database.

## Running the project 

To run the project you can use the `docker-compose.yml` file running the following commands: 

    docker compose up airflow-init
    docker compose up -d

