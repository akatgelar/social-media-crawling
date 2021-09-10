import json
import requests
import pymongo
import urllib
import time
from datetime import datetime
from pymongo import ReturnDocument

# prepare request
USERNAME = ''
KEY = ''
URL_PROFILE = 'https://api.twitter.com/2/'
HEADERS = {
    'Authorization': 'Bearer ' + KEY
}

# prepare mongo
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["vislog"]
mongo_coll_tw_profile = mongo_db["tw_profile"]
mongo_coll_tw_post = mongo_db["tw_post"]
mongo_coll_social_media = mongo_db["social_media"]
mongo_coll_social_media_log = mongo_db["social_media_log"]

try_error = 0


# get profile 
def get_api_profile(param_username):
    param_url = URL_PROFILE + 'users/by' + '?usernames=' + param_username
    param_url += '&user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld'
    response = requests.get(url=param_url, headers=HEADERS)
    response_json = {}
    try:
        response_json = response.json() 

        # insert log
        row_log = {}
        row_log['date'] = datetime.now()
        row_log['category'] = 'twitter'
        row_log['username'] = response_json['data'][0]['username']
        row_log['type'] = 'profile'
        mongo_coll_social_media_log.insert_one(row_log)
        
        # update log social media
        row_log_update = {}
        row_log_update['last_updated_profile'] = datetime.now()
        mongo_coll_social_media.update_one(
            {
                'category': 'twitter',
                'username': response_json['data'][0]['username'],
            },
            {'$set': row_log_update}
        )

        insert_profile(response_json) 

    except Exception as e:
        print(e)
    return response_json
    
# get post 
def get_api_post(is_loop:bool, user_id, next_id, _id):
    param_url = URL_PROFILE + 'users/' + user_id + '/tweets' + '?max_results=10' + '&tweet.fields=author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld'
    if is_loop:
        param_url += '&until_id=' + next_id
    response = requests.get(url=param_url, headers=HEADERS)
    try:
        response_json = response.json()

        result = mongo_coll_tw_profile.find({
            'id': user_id
        }).sort("_id", -1).limit(1)
        for res in result:
            # insert log
            row_log = {}
            row_log['date'] = datetime.now()
            row_log['category'] = 'twitter'
            row_log['username'] = res['username']
            row_log['type'] = 'post'
            mongo_coll_social_media_log.insert_one(row_log)

            # update log social media
            row_log_update = {}
            row_log_update['last_updated_post'] = datetime.now()
            mongo_coll_social_media.update_one(
                {
                    'category': 'twitter',
                    'username': res['username']
                },
                {'$set': row_log_update}
            )

        if 'oldest_id' in response_json['meta']:
            insert_post(response_json['data'], _id)
            get_api_post(True, user_id, response_json['meta']['oldest_id'], _id)
        else:
            insert_post(response_json['data'], _id)

    except Exception as e:
        print(str(e))
        print(response_json)

# insert profile
def insert_profile(datas):
    if datas:
        profile = datas['data'][0]
        row_profile = {}
        row_profile['date'] = datetime.now()
        row_profile['id'] = profile['id']
        row_profile['username'] = profile['username']
        row_profile['name'] = profile['name']
        row_profile['description'] = profile['description']
        row_profile['url'] = profile['url']
        row_profile['profile_image_url'] = profile['profile_image_url']
        row_profile['location'] = profile['location']
        row_profile['protected'] = profile['protected']
        row_profile['verified'] = profile['verified']
        row_profile['created_at'] = datetime.strptime(profile['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
        row_profile['followers_count'] = profile['public_metrics']['followers_count']
        row_profile['following_count'] = profile['public_metrics']['following_count']
        row_profile['tweet_count'] = profile['public_metrics']['tweet_count']
        row_profile['listed_count'] = profile['public_metrics']['listed_count']
        inserted_row_profile = mongo_coll_tw_profile.insert_one(row_profile).inserted_id
        print(inserted_row_profile)
        print(row_profile['username'])
        print()

        get_api_post(False, row_profile['id'], '', inserted_row_profile)

# insert post
def insert_post(datas, _id):
    print(len(datas))
    for p in datas:
        row_post = {}
        row_post['date'] = datetime.now()
        row_post['profile_id'] = _id
        row_post['id'] = p['id']
        row_post['text'] = p['text']
        row_post['source'] = p['source']
        row_post['lang'] = p['lang']
        row_post['created_at'] = datetime.strptime(p['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
        row_post['reply_settings'] = p['reply_settings']
        row_post['possibly_sensitive'] = p['possibly_sensitive']
        row_post['author_id'] = p['author_id']
        row_post['conversation_id'] = p['conversation_id']
        row_post['retweet_count'] = p['public_metrics']['retweet_count']
        row_post['reply_count'] = p['public_metrics']['reply_count']
        row_post['like_count'] = p['public_metrics']['like_count']
        row_post['quote_count'] = p['public_metrics']['quote_count']
        try:
            row_post['hashtags'] = p['entities']['hashtags']
        except Exception as e: 
            row_post['hashtags'] = []
        try:
            row_post['urls'] = p['entities']['urls']
        except Exception as e: 
            row_post['urls'] = []
        
        inserted_row_post = mongo_coll_tw_post.find_one_and_update(
            {'id': row_post['id']},
            {'$set': row_post},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        print(inserted_row_post['_id'])


# select from db
def get_from_db():
    result = mongo_coll_social_media.find({
        'category': 'twitter',
        'is_active': True
    }).sort("_id", -1)
    for res in result:
        param_username = res['username']
        print(param_username)
        get_api_profile(param_username)

def main():
    get_from_db()
    
if __name__ == '__main__':
    main()