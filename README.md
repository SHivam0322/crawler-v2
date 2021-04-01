Requirements: Python3, Rabbitmq, Elasticsearch


To run file use command: python3 FILENAME

To run particular file using pm2 as a cronjob: "pm2 start FILENAME  --interpreter python3 --interpreter-args -u"



This repo Crawls 3 sites named Linkedin, Shine and Indeed for Job scraping.

This repo has 2 main folders i.e. "Publishers" and "Consumers". Publishers Files fetches Job links (using base url present in "jsonFiles" Folder) from above sites and pass it to the respective "*HtmlParse" queues. Consumer files work on these parse queues and fetch particular job data from given link and pass it to "nlp" to find tags from that data using machine learning algorithm and then finally pass it to "mongoJobsSave" queue to save it in mongodb or update the job if already exists.




