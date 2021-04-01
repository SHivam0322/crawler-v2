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
from lib import rabbit_mq, request

load_dotenv()

def fetchData():
    try:
        # print("hello::")

        skill_data = json.loads(os.getenv("SKILL_DATA_COMMON"))
        possible_openings = json.loads(os.getenv("POSSIBLE_OPENINGS_COMMON"))
        skill_count = 0

        for item in skill_data:
            skill_count += 1
            headers = request.set_headers()

            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='linkedin_html_parse')

            print("\n\n\nskill::", item + "\n" + "skill_count::", skill_count)
            for poss in possible_openings:
                print("\n\nposs is ", poss)
                i = 0
                for i in range(int(os.getenv("LINKEDIN_PAGE_LIMIT") or 2)):
                    print("\nIteration " + str(i+1) + " started...")
                    search_keyword = poss+item #item['skillValue'].lower().replace("&"," ")
                    #search_keyword = ' '.join(search_keyword.split()).replace(" ","%20")
                    url = os.getenv("LINKEDIN_SEARCH_URL").replace("keyword_value",search_keyword)
                    url = url+str(25*i)
                    print("url::",url)
                    req = requests.get(url, headers)
                    # soup = BeautifulSoup(req.content, 'html.parser')
                    body = req.content

                    # i += 1

                    print(str(i)+" Linkedin......... iteration data sent to parse queue...")

                    domain = '_'.join(item.split('+')).lower()
                    # print(domain)
            
                    channel.basic_publish(exchange='', routing_key='linkedin_html_parse', body=json.dumps({'html': body.decode("utf-8") , 'domain': domain}))
            
            
            connection.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)

    except Exception as e:
        error = {
            "status": "Linkedin......... Error occured while fetching html",
            "errorMsg": e
        }
        print("Error: ",error)


fetchData()        

# #schedule.every().day.at("08:00").do(fetchData)
# schedule.every(10).seconds.do(fetchData) 

# while True: 
  
#     # Checks whether a scheduled task  
#     # is pending to run or not 
#     schedule.run_pending() 
#     time.sleep(1) 