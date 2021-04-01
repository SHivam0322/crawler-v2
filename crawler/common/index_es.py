from elasticsearch import helpers
from dotenv import load_dotenv
from datetime import datetime
import pika
import json
import hashlib
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, mongo, elastic_search

# curl http://18.208.206.223:9200/_cat/indices?v

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='index_es')


    def callback(ch, method, properties, body):
        index_name = "jobs"
        # 18.208.206.223
        # old_count = 12817 ~ 4
        es = elastic_search.create_connection("18.208.206.223:9200")
        index_exists = es.indices.exists(index=index_name)
        print("\n\nindex_exists::",index_exists)
        # print(" [x] Received %r" % json.loads(body))
        payload = json.loads(body)
        # payload['date'] = datetime.today().strftime('%Y-%m-%d')
        print("payload::", payload)
        job_link = payload['job_link'].encode('utf-8')
        payload['hash_job_link'] = int(hashlib.sha1(job_link).hexdigest(), 16) % (10 ** 8)

        if index_exists:
            data = es.search(index=index_name, body={"query": {"match": {"hash_job_link": payload['hash_job_link']}}})
        if not index_exists or (data and not len(data['hits']['hits'])):
            index = es.index(index=index_name,doc_type='_doc',body=payload)
            print("index::", index)

    channel.basic_consume(
        queue='index_es', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "ES......... Error occured while creating index",
        "errorMsg": e
    }
    print("Error: ",error)
    # message = 'Subject: {}\n\n{}'.format("Gigs4me Job Error", error)
    # mailSent = sendMail(message)
    # print('Main sent: {}'.format(mailSent))