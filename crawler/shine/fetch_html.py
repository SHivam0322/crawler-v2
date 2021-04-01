from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import schedule
import time
import json
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, request, mongo

load_dotenv()

def fetchData():
    try:
        # print("hi")
        client = mongo.create_connection()
        db = mongo.set_db(client, "gigs4me")
        coll = mongo.set_collection(db, "Skills")

        skillData = coll.find()

        skill_count = 0
        for item in skillData:
            skill_count += 1
            # print ("Start : %s" % time.ctime())
            headers = request.set_headers()

            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='shine_html_parse')

            print("\n\n\nskill::", item['skillValue'] + "\n" + "skill_count::", skill_count)
            for i in range(int(os.getenv("SHINE_PAGE_LIMIT") or 2)):
                print("\nIteration " + str(i+1) + " started...")
                search_keyword = item['skillValue'].lower().replace("&"," ")
                search_keyword = ' '.join(search_keyword.split()).replace(" ","-")
                url = os.getenv("SHINE_SEARCH_URL").replace("keyword_value",search_keyword)
                url = url+str(i+1)
                print("url::",url)
                # url = item['url']+str(i+1)
                req = requests.get(url, headers)
                # soup = BeautifulSoup(req.content, 'html.parser')
                body = req.content

                channel.basic_publish(exchange='', routing_key='shine_html_parse', body=body)
                print(str(i+1)+" Shine......... iteration data sent to parse queue...")
            connection.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)

    except Exception as e:
        error = {
            "status": "Shine......... Error occured while fetching html",
            "errorMsg": e
        }
        print("Error: ",error)

fetchData()

# schedule.every(10).seconds.do(fetchData)
# schedule.every().day.at("08:00").do(fetchData)

# while True: 
  
#     # Checks whether a scheduled task  
#     # is pending to run or not 
#     schedule.run_pending() 
#     time.sleep(1) 
