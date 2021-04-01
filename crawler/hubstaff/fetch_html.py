from dotenv import load_dotenv
from bs4 import BeautifulSoup
import schedule
import requests
import time
import json
import sys
import os
from lxml import etree
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, selenium, request, mongo


def fetch_data():
    try:
        # print("hi")
        client = mongo.create_connection()
        db = mongo.set_db(client, "gigs4me")
        coll = mongo.set_collection(db, "domain_airtable")

        skillData = coll.find()

        skill_count = 0
        for item in skillData:
            skill_count += 1
            # if skill_count < 44:
            #     continue
            # print ("Start : %s" % time.ctime())
            headers = request.set_headers()

            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='hubstaff_html_parse')

            print("\n\n\nskill::", item['skillValue'] + "\n" + "skill_count::", skill_count)
            for i in range(int(os.getenv("HUBSTAFF_PAGE_LIMIT") or 2)):
                search_keyword = item['skillValue'].lower().replace("&"," ")
                search_keyword = ' '.join(search_keyword.split()).replace(" ","-")
                url = os.getenv("HUBSTAFF_SEARCH_URL").replace("keyword_value",search_keyword).replace("page_no",str(i+1))
                # url = url+str(10*i)
                print("\nurl::",url)
                driver = selenium.driver()
                try:
                    driver.get(url)
                except Exception as e:
                    print('selenium exception...........continuing next iteration.........')
                    continue
                page = driver.page_source
                soup = BeautifulSoup(page, 'lxml')
                driver.close()
                res = soup.find("div", class_="content-section") if soup.find("div", class_="content-section") else ''
                if len(res) > 0: 
                    # print("\n\n\nres::", res)
                    # print("\n\n\nres::", len(res))
                    jobs = res.find_all("div", class_="search-result") if res.find("div", class_="search-result") else ''
                    print("jobs::", len(jobs))
                    if len(jobs) > 0:
                        domain = '_'.join(item['skillValue'].split(' ')).lower()
                        channel.basic_publish(exchange='', routing_key='hubstaff_html_parse', body=json.dumps({'html':page, 'domain': domain}))
                        print(str(i+1)+" Hubstaff........... iteration data sent to parse queue...")
                    else:
                        print("Search Keyword::", search_keyword)
                        print("No Jobs Data !!")
                        break
                else:
                    print("Search Keyword::", search_keyword)
                    print("No Jobs Data !!")
                    break
            connection.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)


        # skill_data = json.loads(os.getenv("SKILL_DATA_COMMON"))
        # possible_openings = json.loads(os.getenv("POSSIBLE_OPENINGS_COMMON"))
        # skill_count = 0
        
        # for item in skill_data:
        #     skill_count += 1
        #     headers = request.set_headers()

        #     connection = rabbit_mq.create_connection()
        #     channel = connection.channel()
        #     channel.queue_declare(queue='hubstaff_html_parse')

        #     print("\n\n\n\nskill::", item + "\n" + "skill_count::", skill_count)
        #     for poss in possible_openings:        
        #         print("\n\nposs is ", poss)
        #         for i in range(int(os.getenv("HUBSTAFF_PAGE_LIMIT") ) or 2):
        #             print("\nIteration " + str(i+1) + " started...")
        #             search_keyword = poss+item
        #             url = os.getenv("HUBSTAFF_SEARCH_URL").replace("keyword_value",search_keyword).replace("page_no",str(i+1))
        #             print("url::",url)
        #             driver = selenium.driver()
        #             driver.get(url)
        #             page = driver.page_source
        #             soup = BeautifulSoup(page, 'lxml')
        #             driver.close()
        #             # print("\n\n\nsoup::", soup)
        #             res = soup.find("div", class_="content-section") if soup.find("div", class_="content-section") else ''
        #             if len(res) > 0: 
        #                 # print("\n\n\nres::", res)
        #                 # print("\n\n\nres::", len(res))
        #                 jobs = res.find_all("div", class_="search-result") if res.find("div", class_="search-result") else ''
        #                 print("jobs::", len(jobs))
        #                 if len(jobs) > 0:
        #                     body = page
        #                     channel.basic_publish(exchange='', routing_key='hubstaff_html_parse', body=body)
        #                     print(str(i+1)+" Hubstaff........... iteration data sent to consumer!...")
        #                 else:
        #                     print("Search Keyword::", search_keyword)
        #                     print("No Jobs Data !!")
        #                     break
        #             else:
        #                 print("Search Keyword::", search_keyword)
        #                 print("No Jobs Data !!")
        #                 break

        #     channel.close()
        #     time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)
   
    except Exception as e:
        error = {
            "status": "Hubstaff........... Error occured while fetching html",
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
