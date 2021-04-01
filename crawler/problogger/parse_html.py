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

    channel.queue_declare(queue='problogger_html_parse')
    channel.queue_declare(queue='problogger_fetch_job_details')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body, 'html.parser')
        res = soup.find_all("div", class_="wpjb-grid-col wpjb-col-35 wpjb-col-title") if soup.find("div", class_="wpjb-grid-col wpjb-col-35 wpjb-col-title") else ''
        print("\n\n\nJob count before data formation:",len(res))
        print("Creating job links...")
        for item in res:
            job = {}
            job_link = item.find("span", class_="wpjb-line-major") if item.find("span", class_="wpjb-line-major") else ""
            if len(job_link) < 1:
                continue
            job['job_link'] = job_link.find("a").get('href').strip()
            # print("\n\n",job)

            channel.basic_publish(exchange='', routing_key='problogger_fetch_job_details', body=json.dumps(job))

    channel.basic_consume(
        queue='problogger_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'problogger_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "problogger......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",error)