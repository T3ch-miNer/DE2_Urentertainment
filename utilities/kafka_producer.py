import requests
from pprint import pprint
# Import system packages and libraries
from time import sleep
from kafka import KafkaProducer
import json
#from nytWire import get_newsWire
from pprint import pprint

class producer:
  def get_NY_movie_data(url):
        url = url
        query = requests.get(url).json()
        ls = query['results']
        d = []
        for dict in ls:
            data = {}
            data['mpaa_rating'] = dict['mpaa_rating']
            data['display_title'] = dict['display_title']
            data['critics_pick'] = dict['critics_pick']
            # data['url'] = dict['url']
            byline = dict['byline']
            data['headline'] = dict['headline']
            # data['source'] = dict['source']
            data['publication_date'] = dict['publication_date']
            data['summary_short'] = dict['summary_short']
            d.append(data)
        return d

    # Serialize json data for producer
  def json_serializer(data):
      return json.dumps(data).encode('utf-8')

  def fetch_movie_review(title):
      url = 'https://api.nytimes.com/svc/movies/v2/reviews/search.json?query={}&api-key=L3F9t43bEo6WtwYPQwsz9CT7XryDDfzq'.format(title)
      data = get_NY_movie_data(url)
      producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer = json_serializer)
      while 1==1:
        news = data
        for each in news:
            print(each)
            producer.send(title,each)
            sleep(1)