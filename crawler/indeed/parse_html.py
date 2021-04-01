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

    channel.queue_declare(queue='mongo_jobs_save', durable=True)
    channel.queue_declare(queue='indeed_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        body = json.loads(body)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body['html'].encode('utf-8'), 'html.parser')
        res = soup.find_all("div", class_="jobsearch-SerpJobCard")
        print("\n\n\nJob count before data formation:",len(res))
        print("Creating job data...")
        for item in res:
            job = {}
            job['company'] = ''.join(item.find(class_="company").getText().strip()) if item.find(class_="company") else ''
            job['location'] = re.sub(r"[^a-zA-Z0-9()-/@]+", ' ',item.find(class_="location accessible-contrast-color-location").getText().rstrip()) if item.find(class_="location accessible-contrast-color-location") else ''
            job['title'] = item.find(class_="jobtitle").get('title') if item.find(class_="jobtitle") else ''
            job['salary'] = item.find(class_="salaryText").getText().strip() if item.find(class_="salaryText") else ''
            job['job_link'] = 'https://www.indeed.com' + item.find(class_="jobtitle").get('href') if item.find(class_="jobtitle") else ''
            job['link_2'] = ''
            job['experience'] = ''
            job['description'] = item.find("div", class_="summary").getText().strip() if item.find("div", class_="summary") else ''
            job['source'] = 'indeed'
            job['domain'] = body['domain']
            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))

    
    channel.basic_consume(
        queue='indeed_html_parse', on_message_callback=callback, auto_ack=False)
    # channel.basic_consume(callback, 'indeed_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    # exc_type, exc_obj, exc_tb = sys.exc_info()
    # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    error = {
        "status": "Indeed......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",e)
