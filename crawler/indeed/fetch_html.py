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
from lib import rabbit_mq, request, mongo

def fetch_data():
    try: 
        client = mongo.create_connection()
        db = mongo.set_db(client, "gigs4me")
        coll = mongo.set_collection(db, "domain_airtable")

        skill_data = coll.find()
       # skill_data = json.loads(os.getenv("SKILL_DATA_COMMON"))
        possible_openings = json.loads(os.getenv("POSSIBLE_OPENINGS_COMMON"))
        skill_count = 0
        
        for item in skill_data:
            skill_count += 1
            headers = request.set_headers()

            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='indeed_html_parse')

            item = item['skillValue'].lower().replace("&"," ")
            item = ' '.join(item.split()).replace(" ","+")

            print("\n\n\n\nskill::", item + "\n" + "skill_count::", skill_count)
            for poss in possible_openings:        
                print("\n\nposs is ", poss)
                for i in range(int(os.getenv("INDEED_PAGE_LIMIT") ) or 2):
                    print("\nIteration " + str(i+1) + " started...")
                    search_keyword = poss+item
                    #search_keyword = ' '.join(search_keyword.split()).replace(" ","+")
                    url = os.getenv("INDEED_SEARCH_URL").replace("keyword_value",search_keyword)
                    url = url+str(10*i)
                    print("url::",url)
                    # url = item['url']+str(10*i)
                    req = requests.get(url, headers)
                    # soup = BeautifulSoup(req.content, 'html.parser')
                    body = req.content

                    domain = '_'.join(item.split('+')).lower()
                    # print(domain)
                    channel.basic_publish(exchange='', routing_key='indeed_html_parse', body=json.dumps({'html': body.decode("utf-8") , 'domain': domain}))
                    print(str(i+1)+" Indeed........... iteration data sent to parse queue...")

            channel.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)
        
    except Exception as e:
        error = {
            "status": "Indeed........... Error occured while fetching html",
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
