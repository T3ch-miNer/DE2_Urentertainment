# Data Viz Pkg
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
# Hide warnings
import warnings
warnings.filterwarnings('ignore')
import neattext.functions as nfx
from textblob import TextBlob

class senti:
    def get_sentiment(text):
        blob = TextBlob(text)
        sentiment_polarity = blob.sentiment.polarity
        sentiment_subjectivity = blob.sentiment.subjectivity
        if sentiment_polarity > 0:
            sentiment_label = 'Positive'
        elif sentiment_polarity < 0:
            sentiment_label = 'Negative'
        else:
            sentiment_label = 'Neutral'
        result = {'polarity': sentiment_polarity,
                  'subjectivity': sentiment_subjectivity,
                  'sentiment': sentiment_label}
        return result

    def sentiment(data):
        df = pd.DataFrame(data,columns=['text'])

        df['text'].apply(nfx.extract_hashtags)
        df['clean_tweet'] = df['text'].apply(nfx.remove_hashtags)
        df['clean_tweet'] = df['clean_tweet'].apply(lambda x: nfx.remove_userhandles(x))
        df['clean_tweet'] = df['clean_tweet'].apply(nfx.remove_multiple_spaces)
        df['clean_tweet'] = df['clean_tweet'].apply(nfx.remove_urls)
        df['clean_tweet'] = df['clean_tweet'].apply(nfx.remove_puncts)
        df['clean_tweet'] = df['clean_tweet'].apply(nfx.remove_emojis)
        df['sentiment_results'] = df['clean_tweet'].apply(get_sentiment)
        df = df.join(pd.json_normalize(df['sentiment_results']))
        return df[['clean_tweet','sentiment']]



