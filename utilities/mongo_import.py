import pandas as pd
from pymongo import MongoClient
import json

class Mongo_Connection:

    def __init__(self):
        self.client = MongoClient('mongodb+srv://lolwa:lolwa123@cluster0.eoopj.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
        self.database = self.client.HIJK
        self.collection = self.database.kgf2

    def upload(self):
        try:
            self.conn
            # client = pymongo.MongoClient('mongodb+srv://lolwa:lolwa123@cluster0.eoopj.mongodb.net/BDP1?retryWrites=true&w=majority')
        except:
            print('couldnt connect')

        db = self.collection
        input_data = db.kgf2
        data = pd.DataFrame(list(input_data.find()))
        return data