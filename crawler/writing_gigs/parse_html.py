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

    channel.queue_declare(queue='writing_gigs_html_parse')
    channel.queue_declare(queue='writing_gigs_fetch_job_details')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body, 'html.parser')
        res = soup.find_all("div", class_="col-10 col-sm-11") if soup.find("div", class_="col-10 col-sm-11") else ''
        print("\n\n\nJob count before data formation:",len(res))
        print("Creating job links...")
        for item in res:
            job = {}
            job_link = item.find("h5").find("a").get('href').strip() if item.find("h5") else None
            if job_link is None:
                continue
            job['job_link'] = "https://www.flexjobs.com" + job_link

            channel.basic_publish(exchange='', routing_key='writing_gigs_fetch_job_details', body=json.dumps(job))

    channel.basic_consume(
        queue='writing_gigs_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'writing_gigs_html_parse', no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Writing Gigs......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",error)