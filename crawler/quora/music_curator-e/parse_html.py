from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import sys
import re
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='quoraE_fetch_job_details', durable=True)
    channel.queue_declare(queue='quoraE_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("\n\n\ncount::", count)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body, 'html.parser')
        jobs = soup.find_all("div", class_="q-inline") if soup.find("div", class_="q-inline") else ''
        print("\nJob count before data formation:",len(jobs))
        print("Creating job links...")
        for item in jobs:
            job = {}
            job_link = item.getText().replace(" ","-")if item else None
            if job_link is None:
                continue
            job['job_link'] = 'https://www.quora.com/' + job_link
            print("\njob::", job)

            channel.basic_publish(exchange='', routing_key='quoraE_fetch_job_details', body=json.dumps(job))
        else:
            print("no Jobs data !!")

    channel.basic_consume(
        queue='quoraE_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'remote_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Quora......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",e)
