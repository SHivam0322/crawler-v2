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
from lib import rabbit_mq, request

# curl http://18.208.206.223:9200/_cat/indices?v

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='neo4j_save')
    count = 0


    def callback(ch, method, properties, body):
        global count
        count += 1
        # if count < 1527:
        #     return
        print(" \n\n\n[x] Received %r" % json.loads(body))
        payload = json.loads(body)
        res = request.create_nodes(json.dumps({"job":payload}))
        print("\nres::", res)
        print("count::", count)


    channel.basic_consume(
        queue='neo4j_save', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "neo4j......... Error occured while creating neo4j data",
        "errorMsg": e
    }
    print("Error: ",error)
    # message = 'Subject: {}\n\n{}'.format("Gigs4me Job Error", error)
    # mailSent = sendMail(message)
    # print('Main sent: {}'.format(mailSent))