from dotenv import load_dotenv
from bs4 import BeautifulSoup
import feedparser
import schedule
import json
import time
import sys
import os
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, mongo

load_dotenv()

def fetchData():
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
            # print ("Start : %s" % time.ctime())

            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='upwork_html_parse')

            item = item['skillValue'].lower().replace("&"," ")
            item = ' '.join(item.split()).replace(" ","+")
            print("\n\n\n\nskill::", item + "\n" + "skill_count::", skill_count)

            for poss in possible_openings:
                try:
                    print("\n\nposs is ", poss)
                    for i in range(int(os.getenv("UPWORK_PAGE_LIMIT") or 2)):
                        print("\nIteration " + str(i+1) + " started...")
                        search_keyword = poss+item #item['skillValue'].lower().replace("&"," ")
                        #search_keyword = search_keyword.lower().strip().replace(" ", "%20")
                        url = os.getenv("UPWORK_SEARCH_URL").replace("keyword_value",search_keyword)
                        print("url::",url)
                        feeds = feedparser.parse(url)
                        # print("feeds::",feeds)
                        domain = '_'.join(item.split('+')).lower()

                        channel.basic_publish(exchange='', routing_key='upwork_html_parse', body=json.dumps({'feeds':feeds, 'domain': domain}))
                        print(str(i+1)+" Upwork......... iteration data sent to parse queue...")

                except Exception as e:
                    print("Error ocured: ",e, "continuing!!..............")

            connection.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)

    except Exception as e:
        error = {
            "status": "Upwork........... Error occured while getting feed",
            "errorMsg": e
        }
        print("Error: ",error)
        # pass
        
fetchData()


# schedule.every(10).seconds.do(fetchData)
#schedule.every().day.at("08:00").do(fetchData)

#while True:   
    # Checks whether a scheduled task  
    # is pending to run or not 
    #schedule.run_pending() 
    #time.sleep(1)