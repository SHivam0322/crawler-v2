from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import json
import time
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, selenium, request

try:
    headers = requests.utils.default_headers()
    headers.update({ 'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'})

    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='zip_recruiter_fetch_job_details_US', durable=True)
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
        driver.get(job['job_link'])
        time.sleep(10)
        page = driver.page_source
        soup = BeautifulSoup(page, 'lxml')
        # print("soup::", soup)
        driver.close()
        print("Creating job data...")
        try:
            job['postDate'] = soup.find("div", class_="text-muted").getText().strip() if soup.find("div", class_="text-muted") else ''
            job['title'] = soup.find("h1", class_="u-mv--remove u-textH2").getText().strip() if soup.find("h1", class_="u-mv--remove u-textH2") else ''
            job['company'] = soup.find("div", class_="text-primary text-large").getText().strip() if soup.find("div", class_="text-primary text-large") else ''
            location = soup.find("div", class_="u-mt--large") if soup.find("div", class_="u-mt--large") else ''
            if len(location) > 0:
                # print("location:: ", location)
                job['location'] = location.find("div", attrs={'class': None}).getText().strip() if location.find("div", attrs={'class': None}) else ''
                job['location'] = job['location'].split('\n')[0]
            job['salary'] = soup.find("div", class_="job-posting-salary").getText().strip() if soup.find("div", class_="job-posting-salary") else ''
            desc = soup.find("div", class_="job-body").getText().strip() if soup.find("div", class_="job-body") else ''
            job['description'] = ' '.join(desc.split()).replace("=="," ")
            job['source'] = 'zip_recruiter_US'

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))
        except Exception as e:
            print("Zip Recruiter...Unable to parse html::", e)

    # channel.basic_consume(callback, 'zip_recruiter_fetch_job_details_US',  no_ack=False)
    channel.basic_consume(
       queue='zip_recruiter_fetch_job_details_US', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "Zip Recruiter......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)