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

    channel.queue_declare(queue='wwr_job_details', durable=True)
    channel.queue_declare(queue='wwr_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body, 'html.parser')
        data = soup.find("article") if soup.find("article") else None
        job_list = data.find("ul") if data and data.find("ul") else None
        res = job_list.find_all("li") if job_list and job_list.find("li") else ''
        print("\n\n\nJob count before data formation:",len(res))
        print("Creating job links...")
        count = 0
        for item in res:
            job = {}
            urls = item.find_all("a") if item.find("a") else None
            for url in urls:
                job_link = url.get('href')
                if "/remote-jobs" in job_link:
                    count += 1
                    job['job_link'] = "https://weworkremotely.com" + job_link
                    job['company'] = url.find("span", class_="company").getText().strip() if url.find("span", class_="company") else ''
                    print("job::", job)

                    channel.basic_publish(exchange='', routing_key='wwr_job_details', body=json.dumps(job))
        print("Count::", count)

    channel.basic_consume(
        queue='wwr_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'wwr_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "WWR......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",e)
