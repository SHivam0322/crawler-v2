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

    channel.queue_declare(queue='remote_fetch_job_details', durable=True)
    channel.queue_declare(queue='remote_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        body = json.loads(body)
        soup = BeautifulSoup(body['html'].encode('utf-8'), 'html.parser')
        res = soup.find_all("a", class_="card m-0 border-left-0 border-right-0 border-top-0 border-bottom") if soup.find("a", class_="card m-0 border-left-0 border-right-0 border-top-0 border-bottom") else ''
        print("\n\n\nJob count before data formation:",len(res))
        print("Creating job links...")
        for item in res:
            job = {}
            job_link = item.get('href') if item else None
            if job_link is None:
                continue
            job['job_link'] = "https://remote.co" + job_link
            job['domain'] = body['domain']
            # print("job::", job)

            channel.basic_publish(exchange='', routing_key='remote_fetch_job_details', body=json.dumps(job))

    channel.basic_consume(
        queue='remote_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'remote_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Remote......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",e)
