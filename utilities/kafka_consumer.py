from kafka import KafkaConsumer
import json
from pymongo import MongoClient

class consumer:
    client = MongoClient('mongodb+srv://de2_codefellas:lolwa123@cluster0.zobt8p2.mongodb.net/?retryWrites=true&w=majority')
    db = client.moviereview

    def consumer(title):
        consumer = KafkaConsumer(title,bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',group_id="consumer-group-nytWire")
        for msg in consumer:
            print('Hello')
            print('msg')
            record = json.loads(msg.value)
            print(record)
            # display_title = record['display_title']
            rec_id1 = db.Movie
            rec_id1 = db.Movie.insert_one(record)

