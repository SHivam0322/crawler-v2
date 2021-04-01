from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import pika
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))

try:
    headers = requests.utils.default_headers()
    headers.update({ 'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'})


    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='mongo_jobs_save', durable=True)
    channel.queue_declare(queue='problogger_fetch_job_details')

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
        print("Creating job data...")
        try:
            data = soup.find("div", class_="main-content") if soup.find("div", class_="main-content") else ''
            job['title'] = data.find(class_="title").getText().strip() if data.find(class_="title") else ''
            job['company'] = data.find("span", class_="wpjb-top-header-title").getText().strip() if data.find("span", class_="wpjb-top-header-title") else ''
            job['postDate'] = data.find("em", class_="wpjb-top-header-subtitle").getText().strip() if data.find("em", class_="wpjb-top-header-subtitle") else ''
            job['location'] = data.find("div", class_="wpjb-grid-col wpjb-col-65 wpjb-glyphs wpjb-icon-location").getText().strip() if data.find("div", class_="wpjb-grid-col wpjb-col-65 wpjb-glyphs wpjb-icon-location") else ''
            category = soup.find_all("div", class_="wpjb-grid-row") if soup.find("div", class_="wpjb-grid-row") else ''
            for elem in category:
                if "Category" in elem.getText().strip():
                    skills = elem.getText().strip()
                    skills = skills.replace("Category", "")
                    job['skills'] = " ".join(skills.split()).split('/')
            desc = soup.find("div", class_="wpjb-text-box").find("div", class_="wpjb-text").getText().strip() if soup.find("div", class_="wpjb-text-box") else ''
            job['description'] = " ".join(desc.split())
            job['source'] = 'problogger'

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))
        except Exception as e:
            print("problogger...Unable to parse html::", e)

    channel.basic_consume(
        queue='problogger_fetch_job_details', on_message_callback=callback, auto_ack=False)
    # channel.basic_consume(callback, 'problogger_fetch_job_details',  no_ack=False)
    

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "problogger......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)