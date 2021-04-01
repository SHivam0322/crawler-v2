from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import time
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, selenium
count = 0

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='hubstaff_fetch_job_details', durable=True)
    channel.queue_declare(queue='mongo_jobs_save', durable=True)

    def callback(ch, method, properties, body):
        global count
        count += 1
        # if count < 61:
        #     return
        # print("count::", count)
        # print(" [x] Received %r" % json.loads(body))
        job = json.loads(body)
        print("\n\n\njob url::", job['job_link'])
        print("count::", count)
        driver = selenium.driver()
        try:
            driver.get(job['job_link'])
        except Exception as e:
            print('selenium exception...........continuing next iteration.........')
            return
        time.sleep(10)
        page = driver.page_source
        soup = BeautifulSoup(page, 'lxml')
        # print("soup::", soup)
        driver.close()
        print("Creating job data...")
        try:
            job['title'] = soup.find("h1", class_="name").getText().strip() if soup.find("h1", class_="name") else ''
            company_details = soup.find("div", class_="job-company") if soup.find("div", class_="job-company") else None
            # print("company_details::", company_details)
            if company_details is not None:
                job['company'] = soup.find("a", class_="job-agency").getText().strip() if soup.find("a", class_="job-agency") else ''
                job['location'] = soup.find("span", class_="location").getText().strip() if soup.find("span", class_="location") else ''
            skill_data = soup.find("div", id="skills") if soup.find("div", id="skills") else None
            skills = []
            if skill_data is not None:
                skill_list = skill_data.find_all("li") if skill_data.find("li") else []
                for item in skill_list:
                    skill = item.getText().strip()
                    skills.append(skill)
            # print("skills::", skills)
            job['skills'] = skills
            job['description'] = soup.find("div", class_="job-description").getText().strip() if soup.find("div", class_="job-description") else ''
            job['source'] = 'hubstaff'
            print("job::", job)

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))
        except Exception as e:
            print("Hubstaff...Unable to parse html:: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", e)

    # channel.basic_consume(callback, 'hubstaff_fetch_job_details',  no_ack=True)
    channel.basic_consume(
       queue='hubstaff_fetch_job_details', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "Hubstaff......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)