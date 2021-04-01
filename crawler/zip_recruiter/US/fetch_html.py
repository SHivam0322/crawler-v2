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
from lib import rabbit_mq, selenium, request, mongo


def fetchData():
    try:
        # print("hi")
        skill_data = json.loads(os.getenv("SKILL_DATA_COMMON"))
        possible_openings = json.loads(os.getenv("POSSIBLE_OPENINGS_COMMON"))

        skill_count = 0
        for item in skill_data:
            skill_count += 1
            # print ("Start : %s" % time.ctime())
            headers = request.set_headers()

            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='zip_recruiter_html_parse_US')

            print("\n\n\nskill::", item + "\n" + "skill_count::", skill_count)
            crawl = True
            i = 0
            for poss in possible_openings:
                # i += 1
                print("\nIteration " + str(i+1) + " started...")
                search_keyword = poss+item
                url = os.getenv("ZIP_RECUITER_SEARCH_URL_US").replace("keyword_value",search_keyword).replace("page_no",str(i))
                print("url::",url)
                driver = selenium.driver()
                driver.get(url)
                page = driver.page_source
                soup = BeautifulSoup(page, 'lxml')
                driver.close()
                next_page = soup.find("ul", class_="pagination").getText().strip() if soup.find("ul", class_="pagination") else ''
                if "next" not in next_page.lower():
                    crawl = False
                print("crawl:: ", crawl)

                channel.basic_publish(exchange='', routing_key='zip_recruiter_html_parse_US', body=page)
                
                print(str(i+1)+" Zip Recruiter......... iteration data sent to parse queue...")
            connection.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)

    except Exception as e:
        error = {
            "status": "Zip Recruiter......... Error occured while fetching html",
            "errorMsg": e
        }
        print("Error: ",error)

fetchData()

# schedule.every(10).seconds.do(fetchData)
# # schedule.every().day.at("08:00").do(fetchData)

# while True: 
  
#     # Checks whether a scheduled task  
#     # is pending to run or not 
#     schedule.run_pending() 
#     time.sleep(1) 
