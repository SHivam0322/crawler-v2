from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import html
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='mongo_jobs_save', durable=True)
    channel.queue_declare(queue='upwork_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        body = json.loads(body)
        feeds = body['feeds']
        print("\n\n\nJob count before data formation:",len(feeds['entries']))
        print("Creating job data...")
        j = 0
        for feed in feeds['entries']:
            job = {}
            job['title'] = feed['title'].strip() if 'title' in feed else ''
            job['job_link'] = feed['link'].strip() if 'link' in feed else ''
            if 'summary' in feed:
                summary = feed['summary']
                summary = html.unescape(summary)
                desc = summary.split('<b>')
                if len(desc) > 0:
                    desc = desc[0].replace('<br />', " ").strip()
                    desc = " ".join(desc.split())
                job['description'] = desc
                if "<b>Budget</b>" in summary:
                    budget = summary.split('<b>Budget</b>:')[1].split("<b>")[0].replace('<br />', " ").strip()
                    job['budget'] = budget
                if "<b>Posted On</b>" in summary:
                    posted = summary.split('<b>Posted On</b>:')[1].split("<b>")[0].replace('<br />', " ").strip()
                    job['postDate'] = posted
                # if "<b>Category</b>" in summary:
                #     category = summary.split('<b>Category</b>:')[1].split("<b>")[0].replace('<br />', " ").strip()
                #     job['category'] = category
                if "<b>Skills</b>" in summary:
                    skill_list = summary.split('<b>Skills</b>:')[1].split("<b>")[0].replace('<br />', " ")
                    skill_list = " ".join(skill_list.split()).strip()
                    print('skill_list::', skill_list)
                    skill_list = skill_list.split(',')
                    skill_list = [x.strip() for x in skill_list]
                    print('skill_list after::', skill_list)
                    job['skills'] = skill_list
                if "<b>Country</b>" in summary:
                    country = summary.split('<b>Country</b>:')[1].split("<br />")[0].replace('<br />', " ").strip()
                    job['location'] = country
            job['domain'] = body['domain']
            job['source'] = 'upwork'
            print('job::', job)

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))

    channel.basic_consume(
        queue='upwork_html_parse', on_message_callback=callback, auto_ack=False)
    # channel.basic_consume(callback, 'upwork_html_parse', no_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Upwork......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",error)
