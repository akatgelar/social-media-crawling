import json
import requests
import pymongo
import urllib
import time
from datetime import datetime
from facebook_scraper import get_posts
from pymongo import ReturnDocument

# prepare request
USERNAME = ''

# prepare mongo
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["vislog"]
mongo_coll_fb_profile = mongo_db["fb_profile"]
mongo_coll_fb_post = mongo_db["fb_post"]
mongo_coll_social_media = mongo_db["social_media"]
mongo_coll_social_media_log = mongo_db["social_media_log"]

try_error = 0

# get post 
def get_api_post(param_username):
    try:
        # insert log
        row_log = {}
        row_log['date'] = datetime.now()
        row_log['category'] = 'facebook'
        row_log['username'] = param_username
        row_log['type'] = 'post'
        mongo_coll_social_media_log.insert_one(row_log)

        # update log social media
        row_log_update = {}
        row_log_update['last_updated_post'] = datetime.now()
        mongo_coll_social_media.update_one(
            {
                'category': 'facebook',
                'username': param_username,
            },
            {'$set': row_log_update}
        )

        for post in get_posts(param_username, pages=2000, extra_info=True):
            print(param_username)
            insert_post(post, param_username)
    except Exception as e:
        print(e)


# insert post
def insert_post(post, param_username):
    # prepare data
    row_post = {}
    row_post['date'] = datetime.now()
    row_post['username'] = param_username
    row_post['post_id'] = post['post_id']
    row_post['text'] = post['text']
    row_post['post_text'] = post['post_text']
    row_post['shared_text'] = post['shared_text']
    row_post['time'] = post['time']
    row_post['image'] = post['image']
    row_post['video'] = post['video']
    row_post['video_thumbnail'] = post['video_thumbnail']
    row_post['video_id'] = post['video_id']
    row_post['likes'] = post['likes']
    row_post['comments'] = post['comments']
    row_post['shares'] = post['shares']
    row_post['post_url'] = post['post_url']
    row_post['link'] = post['link']
    row_post['user_id'] = post['user_id']
    try:
        row_post['images'] = post['images']
    except Exception as e:
        row_post['images'] = ''
    try:
        row_post['reactions_like'] = post['reactions']['like']
        row_post['reactions_love'] = post['reactions']['love']
        row_post['reactions_support'] = post['reactions']['support']
        row_post['reactions_sorry'] = post['reactions']['sorry']
        row_post['reactions_haha'] = post['reactions']['haha']
        row_post['reactions_wow'] = post['reactions']['wow']
    except Exception as e:
        row_post['reactions_like'] = 0
        row_post['reactions_love'] = 0
        row_post['reactions_support'] = 0
        row_post['reactions_sorry'] = 0
        row_post['reactions_haha'] = 0
        row_post['reactions_wow'] = 0
    try:
        row_post['w3_fb_url'] = post['w3_fb_url']
    except Exception as e:
        row_post['w3_fb_url'] = ''
    
    # execution database
    try:
        inserted_row_post = mongo_coll_fb_post.find_one_and_update(
            {'post_id': row_post['post_id']},
            {'$set': row_post},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        print(inserted_row_post['_id'])
    except Exception as e:
        print(e)

        
# select from db
def get_from_db():
    result = mongo_coll_social_media.find({
        'category': 'facebook',
        'is_active': True
    }).sort("_id", -1)
    for res in result:
        param_username = res['username']
        print(param_username)
        get_api_post(param_username)

def main():
    get_from_db()
    
if __name__ == '__main__':
    main()