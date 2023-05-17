import requests
import json

headers={
    'Content-type':'application/json',
    'Accept':'application/json'
}



new_data = {
    "query": "attack mid center and tackle"
}

json_data = json.dumps(new_data)

url_post = "http://127.0.0.1:80/api/search"

post_response = requests.post(url_post, json=json_data, headers=headers)
print(post_response)

'''

new_data = {
    "query": "tackle through and gain possession",
    "video_id" : int(2841)
}

json_data = json.dumps(new_data)

url_post = "http://127.0.0.1:80/api/add_support"

post_response = requests.post(url_post, json=json_data, headers=headers)
print(post_response)

'''