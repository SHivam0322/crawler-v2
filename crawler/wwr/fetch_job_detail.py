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

    channel.queue_declare(queue='wwr_job_details', durable=True)   
    channel.queue_declare(queue='mongo_jobs_save', durable=True)

    count = 0

    def callback(ch, method, properties, body):
        # print(" [x] Received %r" % json.loads(body))
        time.sleep(5)
        global count
        count += 1
        # if count < 148:
        #     return
        print("count::", count)
        job = json.loads(body)
        print("\n\n\njob url::", job['job_link'])
        req = requests.get(job['job_link'], headers)
        soup = BeautifulSoup(req.content, 'html.parser')
        # print("soup::", soup)
        print("Creating job data...")
        bad_chars = ["&lt", ";", "br", "&", "gt", "/", "div", "ul", "span", "li", "strong"]
        try:
            items = soup.find_all("script", {"type":"application/ld+json"})
            if len(items) > 2:
                data = "".join(items[2].contents)
                # print("data::", data)
                data = json.loads(data, strict=False)
                # print("data::", data)
                job['title'] = data["title"]
                desc = data["description"] if "description" in data else ''
                if len(desc) > 0:
                    for elem in bad_chars:
                        desc = desc.replace(elem, "")
                job['description'] = desc.strip()
                job['postDate'] = data["datePosted"].split(' ')[0] if "datePosted" in data else ''
                job['location'] = data["jobLocation"]["address"]["addressLocality"] if "jobLocation" in data else ''
                job['source'] = 'wwr'
                print("job::", job)

                channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))
        except Exception as e:
            print("WWR...Unable to parse html::", e)

    # channel.basic_consume(callback, 'wwr_job_details',  no_ack=True)
    channel.basic_consume(
       queue='wwr_job_details', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

except Exception as e:
    error = {
        "status": "WWR......... Error occured while fetching job details",
        "errorMsg": e
    }
    print("Error: ",error)