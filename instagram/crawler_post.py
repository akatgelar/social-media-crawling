import json
import requests
import pymongo
import urllib
import time
from datetime import datetime
from pymongo import ReturnDocument

# prepare request
URL_PROFILE = 'https://www.instagram.com/'
URL_POST = 'https://www.instagram.com/graphql/query?query_hash='
HEADERS = {
    'accept': '',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,id;q=0.8',
    'user-agent': '',
    'cookie': 'i'
}

# prepare mongo
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["vislog"]
mongo_coll_ig_profile = mongo_db["ig_profile"]
mongo_coll_ig_post = mongo_db["ig_post"]
mongo_coll_social_media = mongo_db["social_media"]
mongo_coll_social_media_log = mongo_db["social_media_log"]

try_error = 0


# get profile 
def get_api_profile(param_url, param_headers, inserted_row_profile):
    response = requests.get(url=param_url, headers=param_headers)
    response_json = {}
    try:
        response_json = response.json() 
        insert_post(response_json['graphql']['user']['edge_owner_to_timeline_media'], '')
    except Exception as e:
        print(e)
    return response_json
    
# get post 
def get_api_post(param_url, param_headers, param_variables, _id):
    print(param_variables)
    response = requests.get(url=param_url + '&variables=' + param_variables , headers=param_headers)
    response_json = {}
    try:
        response_json = response.json()
        if response_json['status'] == 'ok':
            insert_post(response_json['data']['user']['edge_owner_to_timeline_media'], _id)
        elif response_json['status'] == 'fail' and response_json['message'] == 'rate limited':
            time.sleep(300)
            get_api_post(param_url, param_headers, param_variables, _id)
        else:
            print(response_json)
    except Exception as e:
        if try_error <= 3:
            time.sleep(10)
            get_api_post(param_url, param_headers, param_variables, _id)
        else:
            print(e)

# insert post
def insert_post(datas, _id):
    post = datas['edges']
    print(len(post))

    # insert log
    row_log = {}
    row_log['date'] = datetime.now()
    row_log['category'] = 'instagram'
    row_log['username'] = post[0]['node']['owner']['username']
    row_log['type'] = 'post'
    mongo_coll_social_media_log.insert_one(row_log)

    # update log social media
    row_log_update = {}
    row_log_update['last_updated_post'] = datetime.now()
    mongo_coll_social_media.update_one(
        {
            'category': 'instagram',
            'username': post[0]['node']['owner']['username'],
        },
        {'$set': row_log_update}
    )
    
    for p in post:
        row_post = {}
        row_post['date'] = datetime.now()
        row_post['profile_id'] = _id
        row_post['owner_id'] = p['node']['owner']['id']
        row_post['owner_username'] = p['node']['owner']['username']
        row_post['id'] = p['node']['id']
        row_post['shortcode'] = p['node']['shortcode']
        row_post['typename'] = p['node']['__typename']
        row_post['taken_at_timestamp'] = p['node']['taken_at_timestamp']
        row_post['taken_at_datetime'] = datetime.fromtimestamp(p['node']['taken_at_timestamp'])
        try:
            row_post['media_to_caption'] = p['node']['edge_media_to_caption']['edges'][0]['node']['text']
        except Exception as e:
            row_post['media_to_caption'] = ''
        row_post['is_video'] = p['node']['is_video']
        row_post['comments_disabled'] = p['node']['comments_disabled']
        row_post['media_to_comment'] = p['node']['edge_media_to_comment']['count']
        row_post['media_preview_like'] = p['node']['edge_media_preview_like']['count']
        row_post['display_url'] = p['node']['display_url']
        row_post['location'] = p['node']['location']
        row_post['dimensions_width'] = p['node']['dimensions']['width']
        row_post['dimensions_height'] = p['node']['dimensions']['height']
        row_post['media_overlay_info'] = p['node']['media_overlay_info']
        row_post['media_preview'] = p['node']['media_preview']
        row_post['thumbnail_src'] = p['node']['thumbnail_src']
        try:
            row_post['thumbnail_resources_150'] = p['node']['thumbnail_resources'][0]['src']
            row_post['thumbnail_resources_240'] = p['node']['thumbnail_resources'][1]['src']
            row_post['thumbnail_resources_320'] = p['node']['thumbnail_resources'][2]['src']
            row_post['thumbnail_resources_480'] = p['node']['thumbnail_resources'][3]['src']
            row_post['thumbnail_resources_640'] = p['node']['thumbnail_resources'][4]['src']
        except Exception as e:
            row_post['thumbnail_resources_150'] = ''
            row_post['thumbnail_resources_240'] = ''
            row_post['thumbnail_resources_320'] = ''
            row_post['thumbnail_resources_480'] = ''
            row_post['thumbnail_resources_640'] = ''
        inserted_row_post = mongo_coll_ig_post.find_one_and_update(
            {'id': row_post['id']},
            {'$set': row_post},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        print(inserted_row_post['_id'])

    if datas['page_info']['has_next_page']:
       variables = {}
       variables['id'] = row_post['owner_id']
       variables['first'] = 12
       variables['after'] = datas['page_info']['end_cursor']
       get_api_post(URL_POST, HEADERS, str(variables).replace('\'', '"'), _id)


# select from db
def get_from_db_post():
    result = mongo_coll_social_media.find({
        'category': 'instagram',
        'is_active': True,
    }).sort("last_updated_post", 1).limit(3)
    for res in result:
        param_url = URL_PROFILE + res['username'] + '/?__a=1'
        print(param_url)
        param_header = HEADERS
        get_api_profile(param_url, param_header, res['_id'])


def main():
    get_from_db_post()
    
if __name__ == '__main__':
    main()