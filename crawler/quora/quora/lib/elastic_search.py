from elasticsearch import Elasticsearch as ES

def create_connection(uri="localhost:9200"):
    es = ES(uri)
    return es