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
        headers = request.set_headers()

        connection = rabbit_mq.create_connection()
        channel = connection.channel()

        for i in range(int(os.getenv("PROBLOGGER_PAGE_LIMIT") or 2)):
            print("\n\n\nIteration " + str(i+1) + " started...")
            url = os.getenv("PROBLOGGER_SEARCH_URL")
            url = url+str(i+1)
            print("url::",url)
            req = requests.get(url, headers)
            # soup = BeautifulSoup(req.content, 'html.parser')
            body = req.content

            channel.queue_declare(queue='problogger_html_parse')
            channel.basic_publish(exchange='', routing_key='problogger_html_parse', body=body)
            print(str(i+1)+" Problogger......... iteration data sent to parse queue...")
        connection.close()
        time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)

    except Exception as e:
        error = {
            "errorMsg": e
        }
        print("Error: ",error)

fetchData()

# # schedule.every(10).seconds.do(fetchData)
# schedule.every().day.at("08:00").do(fetchData)

# while True: 
  
#     # Checks whether a scheduled task  
#     # is pending to run or not 
#     schedule.run_pending() 
#     time.sleep(1) 
