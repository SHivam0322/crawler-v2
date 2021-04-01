from dotenv import load_dotenv
from datetime import datetime
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, mongo


try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='mongo_jobs_save', durable=True)

    client = mongo.create_connection()

    def callback(ch, method, properties, body):
        db = mongo.set_db(client)
        coll = mongo.set_collection(db, "QuoraBusinessAnalystcareer")
        # print(" [x] Received %r" % json.loads(body))
        payload = json.loads(body)
        print("\n\n",payload)
        # print(type(payload))

        #coll.insert_many([payload])
        coll.find_one_and_update({'job_link': payload['job_link']}, {'$set': payload, "$setOnInsert": { "date": datetime.utcnow()}}, upsert= True)

    # channel.basic_consume(jobs, 'mongo_jobs_save',  no_ack=False)
    channel.basic_consume(
       queue='mongo_jobs_save', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "Error occured while saving jobs.....",
        "errorMsg": e
    }
    print("Error: ",error)
