from pymongo import MongoClient

def get_db():
    """
    Creates a connection to MongoDB and returns the database.
    """
    client = MongoClient("mongodb://127.0.0.1:27017")
    return client.expense_tracker
