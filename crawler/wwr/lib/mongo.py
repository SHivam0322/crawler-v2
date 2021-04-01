from pymongo import MongoClient
from dotenv import load_dotenv
import os
load_dotenv()


def create_connection():
    client = MongoClient('localhost:27017')
    return client

def set_db(client, db_name=str(os.getenv("MONGO_DB_NAME"))):
    db = client[db_name]
    return db

def set_collection(db, coll_name):
    coll = db[coll_name]
    return coll