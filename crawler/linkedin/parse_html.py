from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    
    channel.queue_declare(queue='mongo_jobs_save', durable=True)
    channel.queue_declare(queue='linkedin_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        body = json.loads(body)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body['html'].encode('utf-8'), 'html.parser')
        res = soup.find_all("li", class_="job-result-card")
        print("\n\n\nJob count before data formation:",len(res))
        print("Creating job data...")
        for item in res:
            job = {}
            job['title'] = item.find(class_="result-card__title").getText().strip() if item.find(class_="result-card__title") else ''
            job['company'] = item.find("h4",class_="result-card__subtitle").getText().strip() if item.find("h4",class_="result-card__subtitle") else ''
            job['location'] = item.find("span", class_="job-result-card__location").getText().strip() if item.find("span", class_="job-result-card__location") else ''
            job['job_link'] = item.find("a", class_="result-card__full-card-link").get('href') if item.find("a", class_="result-card__full-card-link") else ''
            job['link_2'] = item.find("a", class_="job-result-card__subtitle-link").get('href') if item.find("a", class_="job-result-card__subtitle-link") else ''
            job['description'] = ''
            job['salary'] = ''
            job['experience'] = ''
            job['source'] = 'linkedin'
            job['domain'] = body['domain']

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))

    channel.basic_consume(
        queue='linkedin_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'linkedin_html_parse',  no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Linkedin......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",error)
