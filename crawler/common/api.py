from flask import Flask, request, jsonify
from dotenv import load_dotenv
# from flask_api import FlaskAPI
from neo4j import GraphDatabase
import json
import sys
import os

load_dotenv()
sys.path.append(os.path.abspath(os.getenv("system_path")))
app = Flask(__name__)


def create_nodes_relation(payload):
    try:
        print("\n\nn\n::", payload)
        print("\n\nn\n::", type(payload))
        uri = "bolt://localhost:11003"
        driver = GraphDatabase.driver(uri, auth=("neo4j", "admin@123"), encrypted=False)

        db = driver.session()
        job = payload['job']

        tags_relation = {
            "LOCATION": "LOCATED_IN",
            "ORG": "BELONGS_TO",
            "HARDSKILL": "REQUIRES",
            "SOFTSKILL": "REQUIRES",
            "EDUCATIONQUALIFICATION": "REQUIRES",
            "EXPERIENCE": "REQUIRES",
            "TYPEOFWORK": "HAS",
        }

        tags = job['tags']
        print("\n\n\n\n\n\n\n\n\n\n\n\njob::", job)
        # sys.exit()
        del job['tags']
        print(job)
        create_job = "MERGE (j:JOB{job_link:$job_link}) SET j = $job RETURN j"
        job_res = db.run(create_job, job_link = job['job_link'], job = job).data()
        job_id = job_res[0]['j'].id
        print("\n\nid::", job_res[0]['j'].id)
        for tag in tags:
            print("\ntag::", tag)
            node_name = tag[0]
            relation_type = "HAS"
            if node_name in tags_relation:
                relation_type = tags_relation[node_name]
            obj = {
                "slug": '_'.join(tag[1].split()).lower(),
                "display_val": ' '.join(tag[1].split()).title()
            }
            merge_tags = "MATCH (j:JOB) WHERE id(j) = $job_id MERGE (n:"+node_name+ "{slug: $slug, display_val: $display_val}) MERGE (j) - [r:"+relation_type+"] -> (n) RETURN j,r,n"
            res = db.run(merge_tags, node_name = node_name, slug = obj['slug'], display_val = obj['display_val'], job_id = job_id)
            print("res::", res.data())
    except Exception as e:
        raise Exception("Unable to create nodes and relation !!", e)



@app.route('/create_nodes', methods=['POST'])
def example():
    try:
        app.logger.info(request.data)
        response = create_nodes_relation(json.loads(request.data))
        return jsonify(response)
    except Exception as e:
        app.logger.error(e)
        error = {
            "status_code": 400,
            "error": str(e)
        }
        return error, 404

@app.route("/ping")
def hello():
    return "Pong!"


if __name__ == "__main__":
    # app.run(debug=True)
    app.run(host='0.0.0.0', port=5002, debug=True)