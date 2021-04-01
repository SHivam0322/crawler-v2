from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, request


try:
    headers = request.set_headers()
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='mongo_jobs_save', durable=True)
    channel.queue_declare(queue='writing_gigs_fetch_job_details')

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
            data = soup.find("div", class_="col-12 col-lg-9") if soup.find("div", class_="col-12 col-lg-9") else ''
            job['title'] = data.find("h1").getText().strip() if data.find("h1") else ''
            desc = data.find("div", id="job-description").getText().strip() if data.find("div", id="job-description") else ''
            desc = desc.replace("Job Description", "")
            job['description'] = ' '.join(desc.split())
            # postDate
            search_details = data.find("table", class_="table table-striped table-sm").find_all("tr") if data.find("table", class_="table table-striped table-sm") else ''
            for elem in search_details:
                # print("elem::",elem)
                if "Date Posted" in elem.getText():
                    job["postDate"] = elem.find("td").getText().strip() if elem.find("td") else ''
                if "Location:" in elem.getText():
                    location = elem.find("td").getText().strip() if elem.find("td") else ''
                    extra = elem.find("span", class_="sr-only").getText().strip() if elem.find("span", class_="sr-only") else ''
                    if len(extra) > 0:
                        location = location.replace(extra, "")
                    job["location"] = ' '.join(location.split())
                if "Salary & Benefits:" in elem.getText():
                    salary = elem.find("td").getText().strip() if elem.find("td") else ''
                    job["salary"] = ' '.join(salary.split())
                if "Categories" in elem.getText():
                    skills = elem.find("td").getText().strip() if elem.find("td") else ''
                    job['skills'] = ''.join(skills.split()).split(',')
            job['source'] = 'writing_gigs'

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))

        except Exception as e:
            print("problogger...Unable to parse html::", e)

    channel.basic_consume(
        queue='writing_gigs_fetch_job_details', on_message_callback=callback, auto_ack=False)
    
    # channel.basic_consume(callback, 'writing_gigs_fetch_job_details', no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "Writing Gigs......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)