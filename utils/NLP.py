from transformers import pipeline

def sentiment_classification(text: str):
    """Uses an NLP model to classify and return the sentiment of the given text"""
    
    sentiment_analysis = pipeline("sentiment-analysis", model="siebert/sentiment-roberta-large-english")
    sentiment = sentiment_analysis(text)[0].get("label")

    return sentiment 