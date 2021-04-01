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

    channel.queue_declare(queue='no_codery_fetch_job_details', durable=True)   
    channel.queue_declare(queue='mongo_jobs_save', durable=True)

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("\n\n\ncount::", count)
        # print(" [x] Received %r" % json.loads(body))
        job = json.loads(body)
        print("job url::", job['job_link'])
        driver = selenium.driver()
        try:
            driver.get(job['job_link'])
        except Exception as e:
            print('selenium exception...........continuing next iteration.........')
            return
        # time.sleep(10)
        page = driver.page_source
        soup = BeautifulSoup(page, 'lxml')
        # driver.close()
        # print("soup::", soup)
        print("Creating job data...")
        try:
            res = soup.find("div", class_="job-list-details") if soup.find("div", class_="job-list-details") else ''
            if len(res) > 0:
                job['title'] = res.find("h5", class_="title").getText().strip() if res.find("h5", class_="title") else ''
                job['company'] = res.find("span", class_="badge").getText().strip() if res.find("span", class_="badge") else ''
                job['location'] = driver.find_element_by_xpath("//span[@itemprop='address']").text.strip()
                description = soup.find("div", class_="job-details-body").getText() if soup.find("div", class_="job-details-body") else ''
                job['description'] = description.strip()
                if 'Skills:' in description:
                    skills = (description.split('Skills:'))[1].split('\n')[0]
                    skill_list = [x.strip() for x in skills.split(',')]
                    job['skills'] = skill_list
                if 'Posted On:' in description:
                    job['postDate'] = (description.split('Posted On:'))[1].split('UTC')[0] + 'UTC'
            job['source'] = 'no_codery'
            job['domain'] = 'no_code'
            print('job::', job)
            driver.close()

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))
        except Exception as e:
            print("Remote...Unable to parse html::", e)

        # driver.close()

    # channel.basic_consume(callback, 'remote_fetch_job_details',  no_ack=True)
    channel.basic_consume(
       queue='no_codery_fetch_job_details', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "No Codery......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)