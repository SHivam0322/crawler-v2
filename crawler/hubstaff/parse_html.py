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

    channel.queue_declare(queue='hubstaff_fetch_job_details', durable=True)
    channel.queue_declare(queue='hubstaff_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        body = json.loads(body)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body['html'], 'html.parser')
        res = soup.find("div", class_="content-section") if soup.find("div", class_="content-section") else ''
        if len(res) > 0:   
            jobs = res.find_all("div", class_="main-details") if res.find("div", class_="main-details") else ''
            print("\n\n\nJob count before data formation:",len(jobs))
            print("Creating job links...")
            for item in jobs:
                job = {}
                job_link = item.find("a", class_="name").get('href')if item.find("a", class_="name") else None
                if job_link is None:
                    continue
                job['job_link'] = "https://talent.hubstaff.com" + job_link
                job['domain'] = body['domain']

                channel.basic_publish(exchange='', routing_key='hubstaff_fetch_job_details', body=json.dumps(job))
        else:
            print("no Jobs data !!")

    channel.basic_consume(
        queue='hubstaff_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'hubstaff_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Hubstaff......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",e)
