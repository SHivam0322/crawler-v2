from dotenv import load_dotenv
from bs4 import BeautifulSoup
import schedule
import requests
import time
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, request

def fetch_data():
    try: 
        urls = json.loads(os.getenv("REMOTIVE_SEARCH_URL"))
        headers = request.set_headers()

        count = 0
        for url in urls:
            count += 1
            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='remotive_html_parse')

            print("\n\n\nurl::",url,"\nurl count::", count)
            req = requests.get(url, headers)
            soup = BeautifulSoup(req.content, 'html.parser')
            # print("soup::",soup)
            body = req.content
            domain = url.rsplit('/', 1)[-1]

            channel.basic_publish(exchange='', routing_key='remotive_html_parse', body=json.dumps({'html':body.decode("utf-8"), 'domain': domain}))
            print("Remotive data sent to parse queue...")

            channel.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)
        
    except Exception as e:
        error = {
            "status": "Remotive........... Error occured while fetching html",
            "errorMsg": e
        }
        print("Error: ",error)


fetch_data()

#schedule.every().day.at("08:00").do(fetch_data)
# schedule.every(10).seconds.do(fetch_data) 

#while True: 
    # Checks whether a scheduled task  
    # is pending to run or not 
    #schedule.run_pending() 
    #time.sleep(1) 
