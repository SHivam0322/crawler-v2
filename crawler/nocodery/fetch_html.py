from dotenv import load_dotenv
from bs4 import BeautifulSoup
import schedule
import time
import json
import sys
import os
from lxml import etree
load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
from lib import rabbit_mq, selenium

def fetch_data():
    try: 
        urls = json.loads(os.getenv("NO_CODERY_SEARCH_URL"))
        print(len(urls))

        count = 0
        for url in urls:
            count += 1
            connection = rabbit_mq.create_connection()
            channel = connection.channel()
            channel.queue_declare(queue='no_codery_html_parse')

            print("\n\n\nurl count::", count)
            for i in range(int(os.getenv("NO_CODERY_PAGE_LIMIT") or 2)):
                print('i::', i)
                search_url = url.replace("page_no",str(i+1))
                print("\nurl::",search_url)
                driver = selenium.driver()
                try:
                    driver.get(search_url)
                except Exception as e:
                    print('selenium exception...........continuing next iteration.........', e)
                    continue
                time.sleep(5)
                page = driver.page_source
                soup = BeautifulSoup(page, 'lxml')
                driver.close()
                # print("soup::",soup)

                channel.basic_publish(exchange='', routing_key='no_codery_html_parse', body=page)
                print("No Code Founders data sent to parse queue...")

            channel.close()
            time.sleep(int(os.getenv("HTML_FETCH_SLEEP_TIME")) or 60*15)
        
    except Exception as e:
        error = {
            "status": "No Codery........... Error occured while fetching html",
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
