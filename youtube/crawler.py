import json
import requests
import pymongo
import urllib
import time
from datetime import datetime
from pymongo import ReturnDocument

# prepare request
KEY = ''
URL_PROFILE = 'https://www.googleapis.com/youtube/v3/'
HEADERS = {
    'accept': '',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,id;q=0.8',
    'user-agent': ''
}

# prepare mongo
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["vislog"]
mongo_coll_yt_profile = mongo_db["yt_profile"]
mongo_coll_yt_post = mongo_db["yt_post"]
mongo_coll_social_media = mongo_db["social_media"]
mongo_coll_social_media_log = mongo_db["social_media_log"]

try_error = 0


# get profile 
def get_api_profile(param_id):
    param_url = URL_PROFILE + 'channels' + '?key=' + KEY + '&id=' + param_id
    param_url += '&part=brandingSettings&part=contentDetails&part=contentOwnerDetails&part=id&part=localizations&part=snippet&part=statistics&part=status&part=topicDetails'
    response = requests.get(url=param_url)
    response_json = {}
    try:
        response_json = response.json() 
        
        # insert log
        row_log = {}
        row_log['date'] = datetime.now()
        row_log['category'] = 'youtube'
        row_log['username'] = response_json['items'][0]['id']
        row_log['type'] = 'profile'
        mongo_coll_social_media_log.insert_one(row_log)
        
        # update log social media
        row_log_update = {}
        row_log_update['last_updated_profile'] = datetime.now()
        mongo_coll_social_media.update_one(
            {
                'category': 'youtube',
                'userid': response_json['items'][0]['id'],
            },
            {'$set': row_log_update}
        )

        insert_profile(response_json) 

    except Exception as e:
        print(e)
    return response_json
    
# get post 
def get_api_post(is_loop:bool, playlist_id, next_id, _id):
    param_url = URL_PROFILE + 'playlistItems' + '?key=' + KEY + '&playlistId=' + playlist_id + '&maxResults=10' + '&part=snippet'
    if is_loop:
        param_url += '&pageToken=' + next_id
    response = requests.get(url=param_url)
    response_json = {}
    try:
        response_json = response.json()

        # insert log
        row_log = {}
        row_log['date'] = datetime.now()
        row_log['category'] = 'youtube'
        row_log['username'] = response_json['items'][0]['snippet']['channelId']
        row_log['type'] = 'post'
        mongo_coll_social_media_log.insert_one(row_log)

        # update log social media
        row_log_update = {}
        row_log_update['last_updated_post'] = datetime.now()
        mongo_coll_social_media.update_one(
            {
                'category': 'youtube',
                'userid': response_json['items'][0]['snippet']['channelId'],
            },
            {'$set': row_log_update}
        )
        
        if 'nextPageToken' in response_json:
            insert_post(response_json['items'], _id)
            get_api_post(True, playlist_id, response_json['nextPageToken'], _id)
        else:
            insert_post(response_json['items'], _id)
            
    except Exception as e:
        print(str(e))

# insert profile
def insert_profile(datas):
    if datas:
        profile = datas['items'][0]
        row_profile = {}
        row_profile['date'] = datetime.now()
        row_profile['id'] = profile['id']
        row_profile['title'] = profile['snippet']['title']
        row_profile['description'] = profile['snippet']['description']
        try:
            row_profile['customUrl'] = profile['snippet']['customUrl']
        except Exception as e: 
            row_profile['customUrl'] = ''
        row_profile['publishedAt'] = datetime.strptime(profile['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ")
        row_profile['thumbnails_default'] = profile['snippet']['thumbnails']['default']['url']
        row_profile['thumbnails_medium'] = profile['snippet']['thumbnails']['medium']['url']
        row_profile['thumbnails_high'] = profile['snippet']['thumbnails']['high']['url']
        row_profile['country'] = profile['snippet']['country']
        row_profile['viewCount'] = profile['statistics']['viewCount']
        row_profile['subscriberCount'] = int(profile['statistics']['subscriberCount'])
        row_profile['hiddenSubscriberCount'] = profile['statistics']['hiddenSubscriberCount']
        row_profile['videoCount'] = int(profile['statistics']['videoCount'])
        row_profile['privacyStatus'] = profile['status']['privacyStatus']
        row_profile['isLinked'] = profile['status']['isLinked']
        row_profile['longUploadsStatus'] = profile['status']['longUploadsStatus']
        try:
            row_profile['madeForKids'] = profile['status']['madeForKids']
        except Exception as e: 
            row_profile['madeForKids'] = ''
        inserted_row_profile = mongo_coll_yt_profile.insert_one(row_profile).inserted_id
        print(inserted_row_profile)
        print(row_profile['title'])
        print()

        playlistId = profile['contentDetails']['relatedPlaylists']['uploads']
        get_api_post(False, playlistId, '', inserted_row_profile)

# insert post
def insert_post(datas, _id):
    print(len(datas))
    for p in datas:
        row_post = {}
        row_post['date'] = datetime.now()
        row_post['profile_id'] = _id
        row_post['id'] = p['id']
        row_post['publishedAt'] = datetime.strptime(p['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ")
        row_post['channelId'] = p['snippet']['channelId']
        row_post['title'] = p['snippet']['title']
        row_post['description'] = p['snippet']['description']
        row_post['thumbnails_default'] = p['snippet']['thumbnails']['default']['url']
        row_post['thumbnails_medium'] = p['snippet']['thumbnails']['medium']['url']
        row_post['thumbnails_high'] = p['snippet']['thumbnails']['high']['url']
        try:
            row_post['thumbnails_standard'] = p['snippet']['thumbnails']['standard']['url']
        except Exception as e: 
            row_post['thumbnails_standard'] = ''
        try:
            row_post['thumbnails_maxres'] = p['snippet']['thumbnails']['maxres']['url']
        except Exception as e: 
            row_post['thumbnails_maxres'] = ''
        row_post['channelTitle'] = p['snippet']['channelTitle']
        row_post['playlistId'] = p['snippet']['playlistId']
        row_post['position'] = p['snippet']['position']
        row_post['resourceId_kind'] = p['snippet']['resourceId']['kind']
        row_post['resourceId_videoId'] = p['snippet']['resourceId']['videoId']

        try:
            param_url = URL_PROFILE + 'videos' + '?key=' + KEY + '&id=' + p['snippet']['resourceId']['videoId']
            param_url += '&part=contentDetails&part=statistics'
            response = requests.get(url=param_url)
            response_json = response.json()
            response_json = response_json['items'][0]

            row_post['duration'] = response_json['contentDetails']['duration']
            row_post['dimension'] = response_json['contentDetails']['dimension']
            row_post['definition'] = response_json['contentDetails']['definition']
            row_post['caption'] = response_json['contentDetails']['caption']
            row_post['licensedContent'] = response_json['contentDetails']['licensedContent']
            row_post['projection'] = response_json['contentDetails']['projection']
            row_post['viewCount'] = int(response_json['statistics']['viewCount'])
            row_post['likeCount'] = int(response_json['statistics']['likeCount'])
            row_post['dislikeCount'] = int(response_json['statistics']['dislikeCount'])
            row_post['favoriteCount'] = int(response_json['statistics']['favoriteCount'])
            row_post['commentCount'] = int(response_json['statistics']['commentCount'])
        except Exception as e:
            print(e)
            row_post['duration'] = ''
            row_post['dimension'] = ''
            row_post['definition'] = ''
            row_post['caption'] = ''
            row_post['licensedContent'] = ''
            row_post['projection'] = ''
            row_post['viewCount'] = ''
            row_post['likeCount'] = ''
            row_post['dislikeCount'] = ''
            row_post['favoriteCount'] = ''
            row_post['commentCount'] = ''

        inserted_row_post = mongo_coll_yt_post.find_one_and_update(
            {'id': row_post['id']},
            {'$set': row_post},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        print(inserted_row_post['_id'])


# select from db
def get_from_db():
    result = mongo_coll_social_media.find({
        'category': 'youtube',
        'is_active': True
    }).sort("_id", -1)
    for res in result:
        param_id = res['userid']
        print(param_id)
        get_api_profile(param_id)


def main():
    get_from_db()
    
    
if __name__ == '__main__':
    main()