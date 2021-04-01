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

    channel.queue_declare(queue='remotive_fetch_job_details', durable=True)
    channel.queue_declare(queue='remotive_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("\n\n\ncount::", count)
        # print(" [x] Received %r" % body)
        body = json.loads(body)
        soup = BeautifulSoup(body['html'].encode('utf-8'), 'html.parser')
        res = soup.find_all("div", class_="job-list-item-content") if soup.find("div", class_="job-list-item-content") else ''
        print("Job count before data formation:",len(res))
        print("Creating job links...")
        for item in res:
            job = {}
            job_link = item.find("div", class_="position") if item.find("div", class_="position") else None
            url = job_link.find("a").get('href') if job_link and job_link.find("a") else None
            if url is None:
                print('no job url ..... continue next iteration')
                continue
            job['job_link'] = "https://remotive.io" + url
            location = item.find("span", class_="location") if job_link and item.find("span", class_="location") else None
            job['location'] = location.find("span").getText().strip("( )") if location and location.find("span") else ''
            job['domain'] = body['domain']
            # print('job::', job)
            channel.basic_publish(exchange='', routing_key='remotive_fetch_job_details', body=json.dumps(job))

    channel.basic_consume(
        queue='remotive_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'remotive_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Remotive......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",e)
