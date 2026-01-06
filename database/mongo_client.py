import os
from pymongo import MongoClient

def get_db():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    client = MongoClient(mongo_uri)
    return client["expense_tracker"]
