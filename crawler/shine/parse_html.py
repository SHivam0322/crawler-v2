from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pika
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, request, mongo

try:
    connection = rabbit_mq.create_connection()
    channel = connection.channel()

    channel.queue_declare(queue='mongo_jobs_save', durable=True)
    channel.queue_declare(queue='shine_html_parse')

    count = 0

    def callback(ch, method, properties, body):
        global count
        count += 1
        print("count::", count)
        # print(" [x] Received %r" % body)
        soup = BeautifulSoup(body, 'html.parser')
        res = soup.find_all("li", class_="search_listingleft search_listingleft_100")
        print("\n\n\nJob count before data formation:",len(res))
        print("Creating job data...")
        j = 0
        for item in res:
            # print(j)
            job = {}
            job['title'] = item.find("li",class_="snp cls_jobtitle").getText().strip() if item.find("li",class_="snp cls_jobtitle") else ''
            job['company'] = item.find("li",class_="snp_cnm cls_cmpname cls_jobcompany").getText().strip() if item.find("li",class_="snp_cnm cls_cmpname cls_jobcompany") else ''
            job['experience'] = item.find("span", class_="snp_yoe cls_jobexperience").getText().strip() if item.find("span", class_="snp_yoe cls_jobexperience") else ''
            job['description'] = item.find("li", class_="srcresult").getText().strip() if item.find("li", class_="srcresult") else ''
            job['location'] = item.find("em", class_="snp_loc").getText().strip() if item.find("em", class_="snp_loc") else ''
            # job['skills'] = item.find(["div", "span", "mark"], class_="sk jsrp cls_jobskill").getText().strip() if item.find(["div", "mark"], class_="sk jsrp cls_jobskill") else ''
            job['job_link'] = "https://www.shine.com" + item.find("a", class_="cls_searchresult_a searchresult_link").get('href') if item.find("a", class_="cls_searchresult_a searchresult_link") else ''
            job['link_2'] = ''
            job['salary'] = ''
            job['source'] = 'shine'
            skillsData = item.find(["div", "sk jsrp cls_jobskill"]) if item.find(["div", "sk jsrp cls_jobskill"]) else ''
            if len(skillsData) > 0:
                spans = skillsData.find_all('span') if skillsData.find_all('span') else ''
                if len(spans) > -1:
                    skills = []
                    for span in spans:
                        skills.append(span.getText().strip())
                    skills.pop(0)
            job['skills'] = skills

            channel.basic_publish(exchange='', routing_key='mongo_jobs_save', body=json.dumps(job))


    channel.basic_consume(
        queue='shine_html_parse', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(callback, 'shine_html_parse',  no_ack=False)
    

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except Exception as e:
    error = {
        "status": "Shine......... Error occured while parsing html",
        "errorMsg": e
    }
    print("Error: ",error)
