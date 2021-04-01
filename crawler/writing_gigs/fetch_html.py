from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import schedule
import json
import time
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, request


def fetchData():
    try:
        jobsJson = open('writingGigsJobs.json', 'r')
        jobsData = json.loads(jobsJson.read())
        count = 0
        for item in jobsData['jobs']:
            count += 1
            headers = request.set_headers()

            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='writing_gigs_html_parse')
            
            print("\n\n\ncount::", count)
            for i in range(int(os.getenv("WRITINGGIGS_PAGE_LIMIT") or 2)):
                print("\nIteration " + str(i+1) + " started...")
                url = item['url']
                url = url+str(i+1)
                print("url::",url)
                req = requests.get(url, headers)
                # soup = BeautifulSoup(req.content, 'html.parser')
                body = req.content

                channel.basic_publish(exchange='', routing_key='writing_gigs_html_parse', body=body)
                print(str(i+1)+" Writing Gigs......... iteration data sent to parse queue...")
            connection.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)

    except Exception as e:
        error = {
            "status": "Writing Gigs......... Error occured while fetching html",
            "errorMsg": e
        }
        print("Error: ",error)

fetchData()

# schedule.every(10).seconds.do(fetchData)
# #schedule.every().day.at("08:00").do(fetchData)

# while True: 
  
#     # Checks whether a scheduled task  
#     # is pending to run or not 
#     schedule.run_pending() 
#     time.sleep(1) 
