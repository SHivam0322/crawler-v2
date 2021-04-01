from neo4j import GraphDatabase
import requests
import json

headers = {
        'Content-Type': 'application/json'
}
url = "https://api.gigsfor.me/api/Profiles"
response = requests.request("GET", url, headers=headers)


if response.status_code == 200:
    count = 0
    fiver_skill_url = "http://localhost:5001/fiver_skill"
    profiles = json.loads(response.content)
    break_case = False
    uri = "bolt://localhost:11003"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "admin@123"), encrypted=False)
    db = driver.session()
    for profile in profiles:
        count += 1
        print("\n\n\n\nuser_name:: %s \nskills:: %s \ncount:: %d " %   (profile['userName'],  profile['skills'], count))

        create_user = "MERGE (u:USER{user_name:$userName}) RETURN u"
        user_res = db.run(create_user, userName = profile['userName']).data()
        user_id = user_res[0]['u'].id
        print("id::", user_id)

        for item in profile['skills']:
            slug = '_'.join(item.split()).lower()
            print("\nslug::", slug)
            body = {"user_subdomain":slug}
            # print("body::", body)
            res = requests.request("POST", fiver_skill_url, headers=headers, data = json.dumps(body))
            if res.status_code == 200:
                fiver_skill = json.loads(res.content)
                # print(fiver_skill)
                if len(fiver_skill.items()) > 0:
                    (key, val), = fiver_skill.items()
                    tags = {k: v for k, v in sorted(fiver_skill[key].items(), key=lambda item: item[1], reverse= True)[:10]}.keys()
                    print("\n\n tags::", tags)



                    
                    
                    for tag in tags:
                        print("\n\ntag::", tag)
                        relation_type = "HAS"
                        obj = {
                            "slug": tag,
                            "display_val": ' '.join(tag.split("_")).title()
                        }
                        merge_tags = "MATCH (u:USER) WHERE id(u) = $user_id MERGE (n:HARDSKILL{slug: $slug, display_val: $display_val}) MERGE (u) - [r:"+relation_type+"] -> (n) RETURN u,r,n"
                        res = db.run(merge_tags, slug = obj['slug'], display_val = obj['display_val'], user_id = user_id)
                        print("res::", res.data())

        #             break_case = True
        #             # break
        # if break_case:
        #     break

# x = {'ability_to_work_under_pressure': 0.0471253534401508, 'abode_photoshop': 0.0471253534401508, 'accounting': 0.1413760603204524, 'accounts_payable': 0.0471253534401508, 'accounts_receivable': 0.0471253534401508, 'ad_posting': 0.0471253534401508, 'admin_support': 0.2827521206409048, 'administration': 0.5655042412818096, 'administrative_assistant': 0.3298774740810556, 'adobe_acrobat': 0.0471253534401508, 'adobe_audition': 0.0471253534401508, 'adobe_dreamweaver': 0.0471253534401508, 'adobe_ilustrator': 0.4241281809613572, 'adobe_indesign': 0.1885014137606032, 'adobe_photoshop': 0.8953817153628653, 'adobe_premiere_pro': 0.0471253534401508, 'adobe_suite': 0.1413760603204524, 'advanced_excel': 0.1885014137606032}
# # 
# # x = {1: 2, 3: 4, 4: 3, 2: 1, 0: 0}

# print("\n\n\n\n:: before", x)

# y ={k: v for k, v in sorted(x.items(), key=lambda item: item[1], reverse= True)[:10]}

# print("\n\n\n\n after::", y)