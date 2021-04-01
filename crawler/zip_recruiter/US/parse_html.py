from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pika
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

    channel.queue_declare(queue='zip_recruiter_html_parse_US')
    channel.queue_declare(queue='zip_recruiter_fetch_job_details_US', durable=True)

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body, 'lxml')
        #res = soup.find_all("a", class_="jobList-title zip-backfill-link") if soup.find("a", class_="jobList-title zip-backfill-link") else ''
        res = soup.find_all("a", class_="jobList-title zip-backfill-link") if soup.find("a", class_="jobList-title zip-backfill-link") else ''
        
        print("\n\n\nJob count before data formation:",len(res))
        print("Creating job links...")
        for item in res:
            job = {}
            job_link = item.get('href').strip() if item else None
            if job_link is None:
                continue
            job['job_link'] = job_link

            channel.basic_publish(exchange='', routing_key='zip_recruiter_fetch_job_details_US', body=json.dumps(job))

    # channel.basic_consume(callback, 'zip_recruiter_html_parse_US',  no_ack=False)
    channel.basic_consume(
       queue='zip_recruiter_html_parse_US', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Zip Recruiter......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",error)