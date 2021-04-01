from dotenv import load_dotenv
import requests
import json
import os
load_dotenv()

def set_headers():
    headers = requests.utils.default_headers()
    headers.update({ 'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'})
    return headers


def get_tags(body):
    nlp_url = os.getenv("NLP_URL")
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", nlp_url, headers=headers, data = body)
    tags = []
    # print("response.status_code", response.status_code)
    if response.status_code == 200:
        tags = json.loads(response.content)    
        print("\ntags from api", tags)
    return tags

def create_nodes(body):
    nlp_url = os.getenv("NEO4J_API_URL")
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", nlp_url, headers=headers, data = body)
    return response


# def get_tags(data):
#     jd_url = os.getenv("JD_URL")
#     headers = {
#             'Content-Type': 'application/json'
#                 }
#     response = requests.request("POST", jd_url, headers=headers, data = data)
#     tags = {}
#     if response.status_code == 200:
#         tags.update(json.loads(response.content))
#     return tags