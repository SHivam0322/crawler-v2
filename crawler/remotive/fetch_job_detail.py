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

    channel.queue_declare(queue='remotive_fetch_job_details', durable=True)
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
            content = soup.find("div", class_="content") if soup.find("div", class_="content") else None
            if content is not None:
                job['title'] = content.find("h1").getText().strip() if soup.find("h1") else ''
                job['company'] = content.find("h2", class_="company").getText().strip() if soup.find("h2", class_="company") else ''
                skill_data = soup.find("div", class_="job-tags") if soup.find("div", class_="job-tags") else None
                skills = []
                skill_list = skill_data.find_all("a") if skill_data and skill_data.find("a") else []
                for item in skill_list:
                    skill = item.getText().strip()
                    skills.append(skill)
                # print("skills::", skills)
                job['skills'] = skills
                job['description'] = soup.find("div", class_="job-description").getText().strip() if soup.find("div", class_="job-description") else ''
                job['source'] = 'remotive'
                print("job::", job)

                channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))
            else:
                print("No data !!")
        except Exception as e:
            print("Remotive...Unable to parse html::", e)

    # channel.basic_consume(callback, 'remotive_fetch_job_details',  no_ack=True)
    channel.basic_consume(
       queue='remotive_fetch_job_details', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "Remotive......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)