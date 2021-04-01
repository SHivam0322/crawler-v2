from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import json
import time
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, selenium

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='crossover_fetch_job_details', durable=True)   
    channel.queue_declare(queue='mongo_jobs_save', durable=True)

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % json.loads(body))
        job = json.loads(body)
        print("\n\n\njob url::", job['job_link'])
        driver = selenium.driver()
        try:
            driver.get(job['job_link'])
        except Exception as e:
            print('selenium exception...........continuing next iteration.........')
            return
        time.sleep(10)
        page = driver.page_source
        soup = BeautifulSoup(page, 'lxml')
        driver.close()
        # print("soup::", soup)
        print("Creating job data...")
        try:
            job['title'] = soup.find("h5", class_="pipeline-name").getText() if soup.find("h5", class_="pipeline-name") else ''
            job['company'] = soup.find("div", class_="pipeline-desc").get("a") if soup.find("div", class_="pipeline-desc") else ''
            job['location'] = "Remote"
            description = soup.find_all("div", class_="pipeline-content-section") if soup.find("div", class_="pipeline-content-section") else None
            job['description'] = ''
            i = 0
            for item in description:
                i += 1
                if i == 3 or i == 5 or i == 7:
                    job['description'] = job['description'] + ' ' + item.getText().strip()
            job['description'] = job['description'].strip()
            job['source'] = 'crossover'
            print("job::", job)
            
            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))
        except Exception as e:
            print("Remote...Unable to parse html::", e)

    # channel.basic_consume(callback, 'remote_fetch_job_details',  no_ack=True)
    channel.basic_consume(
       queue='crossover_fetch_job_details', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "Crossover......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)