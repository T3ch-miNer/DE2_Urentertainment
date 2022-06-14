import streamlit as st
from datetime import date
from plotly import graph_objs as go
import pandas as pd
import requests
from utilities.kafka_consumer import consumer
from utilities.kafka_producer import producer
from pymongo import MongoClient
from utilities.senti import senti
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
# Hide warnings
import warnings
warnings.filterwarnings('ignore')
import neattext.functions as nfx
from textblob import TextBlob
import plotly.express as px

#from api import movieApi

html_temp = """
<div style="background-color:tomato;padding:1.5px">
<h1 style="color:white;text-align:center;">UR entertainment </h1>
</div><br>"""


st.markdown(html_temp,unsafe_allow_html=True)
genres = ("Action","Thriller","Horror","Comedy","Animation","Fantasy","Adventure","Romance","Crime")
#selected_genre = st.selectbox("Which genre would you like to watch?",genres)
#@st.experimental_memo(ttl=6)
client = MongoClient("localhost", 27017, maxPoolSize=50)
db = client.DE2
collection = db.Movie
df = pd.DataFrame(list(collection.find()))
df = df.astype({"_id": str})
df['title'].str.strip().str.split(' ').str.len().eq(1)
#df["id"] = df["id"].astype(float).astype(int)
#df['title'] = df.rename(columns = {'original_title':'title'}, inplace = True)

#df = pd.read_csv(r"./data_files/data1.csv")
#df['id'] =df['id'].astype(int)
genres = ("Action","Thriller","Horror","Comedy","Animation","Fantasy","Adventure","Romance","Crime")

def fetch_poster(movies_id):
    response = requests.get('https://api.themoviedb.org/3/movie/{''}?api_key=e0f996ebad1f3ec1f2e9e65290388327&language=en-US'.format(movies_id))
    data = response.json()
    return "https://image.tmdb.org/t/p/w500/" + data['poster_path']



sector = df.groupby('genres')
selected_sector = st.sidebar.multiselect('Which genre would you like to watch',genres)
df_selected = df[(df['genres'].isin(selected_sector))].sort_values(by=['popularity'], ascending=False).head(10)
#st.dataframe(df_selected)
#st.dataframe(df)
decide = st.sidebar.radio("These are the top 10 trending movies of your selected genres", options=df_selected["title"])
#st.text(decide.replace(" ", ""))
#st.header(decide)
#image = fetch_poster(df['id'][df['title']==decide])
def fetch_poster(movie_id):
    url = "https://api.themoviedb.org/3/movie/{}?api_key=8265bd1679663a7ea12ac168da84d2e8&language=en-US".format(
        movie_id)
    data = requests.get(url)
    data = data.json()
    poster_path = data['poster_path']
    full_path = "https://image.tmdb.org/t/p/w500/" + poster_path
    return full_path

#movie_id = int(df['id'][df['title'] == decide])

if decide:
    try:
        movie_id = int(df['id'][df['title'] == decide])
        url = "https://api.themoviedb.org/3/movie/{}?api_key=8265bd1679663a7ea12ac168da84d2e8&language=en-US".format(movie_id)
        data = requests.get(url)
        data = data.json()
        col1, col2 = st.columns([1, 2])
        with col1:
            st.image(fetch_poster(movie_id))
        with col2:
        #titlee= print(df['title'][df['title']== decide])
            st.subheader(data['title'])
            st.write(data['overview'])
    except:
        st.error('No movie title found')
    #except:
       # st.error('no movie with that title')
#movie_id = print(df['id'][df['title'] == decide])
#st.dataframe(df_selected)


Courses = list(df_selected['title'])
values = list(df_selected['budget'])

fig = plt.figure(figsize = (10, 5))

plt.bar(Courses, values)
plt.xlabel("Movies")
plt.ylabel("Revenue")
plt.title("Distribution of revenue of top 10 movies of {} Genre ".format(selected_sector))
st.pyplot(fig)


if st.button("sentiment analysis"):

    #df1 = df1.astype({"_id": str})

    path = r'./tweets/{}.csv'.format(decide)
    result = pd.read_csv(path)


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
    sen = sentiment(result)

    fig = plt.figure(figsize=(10, 4))
    sns.countplot(x="sentiment", data=sen)
    st.pyplot(fig)
    #st.dataframe(sen)




