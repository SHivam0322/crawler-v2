from neo4j import GraphDatabase
import requests
import pandas
import json
import sys

data = pandas.read_csv('crawler/common/usertojobs_manual_data.csv')

usernames = data.username.tolist()

# print(set(usernames))
# print(len(set(usernames)))
# sys.exit()

headers = {
        'Content-Type': 'application/json'
}
url = "https://api.gigsfor.me/api/Profiles/"

fiver_base_url = "http://localhost:5001/"

# obj = {}
count = 0


uri = "bolt://localhost:11003"
driver = GraphDatabase.driver(uri, auth=("neo4j", "admin@123"), encrypted=False)
db = driver.session()

for username in usernames:
    count += 1
    # obj[username] = {}
    print("\n\n\n\n\n\n\nusername::", username)
    print("count::", count)


    create_user = "MERGE (u:USER{user_name:$userName}) RETURN u"
    user_res = db.run(create_user, userName = username).data()
    user_id = user_res[0]['u'].id
    print("id::", user_id)


    response = requests.request("GET", url+username, headers=headers)
    # print("response.status_code::", response.status_code)
    if response.status_code == 200:
        profile = json.loads(response.content)
        print("profile::",profile)
    for elem in profile['skills']:
        # obj[username][item] = {}

        slug_similar_skill = '_'.join(elem.split()).lower()
        print("\n\nslug_similar_skill::", slug_similar_skill)
        payload = {"user_skill":slug_similar_skill}
        similar_skills = requests.request("POST", fiver_base_url+"similar_skills", headers=headers, data = json.dumps(payload))
        if similar_skills.status_code == 200:
            skills = json.loads(similar_skills.content)
            for skill in skills.keys():
                print("\nskill::",skill)
                # slug = '_'.join(skill.split()).lower()
                # print("slug::", slug)
                body = {"user_subdomain":skill}
                res = requests.request("POST", fiver_base_url+"fiver_skill", headers=headers, data = json.dumps(body))
                if res.status_code == 200:
                    fiver_skill = json.loads(res.content)
                    # print(fiver_skill)
                    # print(len(fiver_skill['technical_writing'].items()))
                    if len(fiver_skill.items()) > 0:
                        (key, val), = fiver_skill.items()
                        tags = {k: v for k, v in sorted(fiver_skill[key].items(), key=lambda item: item[1], reverse= True)[:10]}
                        print("\n tags::", tags)


                        for tag in tags.keys():
                            print("\ntag::", tag)
                            relation_type = "HAS"
                            obj = {
                                "slug": tag,
                                "display_val": ' '.join(tag.split("_")).title()
                            }
                            merge_tags = "MATCH (u:USER) WHERE id(u) = $user_id MERGE (n:HARDSKILL{slug: $slug, display_val: $display_val}) MERGE (u) - [r:"+relation_type+"] -> (n) RETURN u,r,n"
                            res = db.run(merge_tags, slug = obj['slug'], display_val = obj['display_val'], user_id = user_id)
                            print("res::", res.data())


                # obj[username][item] = tags
                # break
    # print("/n/n/nobj::", obj)
    # break
