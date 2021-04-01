from datetime import datetime, timedelta
from elasticsearch import helpers
from dotenv import load_dotenv
from datetime import datetime
import hashlib
import pika
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, mongo, elastic_search


def delete_old_indexes():
    index_name = "jobs"
    es = elastic_search.create_connection("18.208.206.223:9200")
    index_exists = es.indices.exists(index=index_name)
    print("\n\nindex_exists::",index_exists)

    if index_exists:    
        start_date = datetime.today() + timedelta(days=1)
        print("start_date::", start_date)
        start_date = (start_date - datetime(1970,1,1)).total_seconds()
        print("Epoch Date::", start_date)
        response = es.delete_by_query(index=index_name, body= { "query" : {"range" : { "date" : { "lte" : start_date } } } }, ignore=[400, 404])
        # print("status::", response.status_code)
        print(response)
    else:
        Print("index not exists !!")


delete_old_indexes()