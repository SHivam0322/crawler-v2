from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import json
import time
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, request

try:
    headers = request.set_headers()
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='remote_fetch_job_details', durable=True)   
    channel.queue_declare(queue='mongo_jobs_save', durable=True)

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % json.loads(body))
        job = json.loads(body)
        print("\n\n\njob url::", job['job_link'])
        req = requests.get(job['job_link'], headers)
        soup = BeautifulSoup(req.content, 'html.parser')
        # print("soup::", soup)
        print("Creating job data...")
        try:
            title = soup.find("h1", class_="font-weight-bold").getText().strip() if soup.find("h1", class_="font-weight-bold") else ''
            title_list = title.split(" at ") if title else []
            job['title'] = title_list[0] if len(title_list) > 0 else ''
            job['company'] = title_list[1] if len(title_list) > 1 else ''
            job['location'] = "Remote"
            job['description'] = soup.find("div", class_="job_description").getText().strip() if soup.find("div", class_="job_description") else ''
            job['source'] = 'remote'
            print("job::", job)

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))
        except Exception as e:
            print("Remote...Unable to parse html::", e)

    # channel.basic_consume(callback, 'remote_fetch_job_details',  no_ack=True)
    channel.basic_consume(
       queue='remote_fetch_job_details', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "Remote......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)