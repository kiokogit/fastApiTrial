import os

from pymongo import MongoClient


db_host = os.environ['MONGO_HOST']
mongo_port = os.environ['MONGO_PORT']


client = MongoClient(os.environ["MONGODB_URL"])
database = client[os.environ["MONGODB_DATABASE"]]
