
# For sending GET requests from the API
import requests

# For saving access tokens and for file management when creating and adding to the dataset
import os

# For dealing with json responses we receive from the API
import json

# For displaying the data after
import pandas as pd

# For saving the response data in CSV format
import csv

# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata

#To add wait time between requests
import time

#To open up a port to forward tweets
import socket 

os.environ['TOKEN'] =  ""

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword + ' -is:retweet  -is:reply',
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified,location',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "layoffs OR layoff OR Job loss   lang:en"
max_results = 100

s = socket.socket()
host = "127.0.0.1"
port = 7777
s.bind((host, port))
print("Listening on port: %s" % str(port))
s.listen(5)
clientsocket, address = s.accept()
print("Received request from: " + str(address)," connection created.")

while True:
       # Set start_time and end_time will be rerenced from one day before (day-1)
    now = time.time()
    start_time = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now - ((24 * 60 * 60))))
    end_time = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now - ((24 * 60 * 60)-300)))
    
    url = create_url(keyword, start_time,end_time, max_results)
    json_response = connect_to_endpoint(url[0], headers, url[1])
    
    if json_response is not None:
        if "includes" in json_response:
            includes = json_response["includes"]
        else:
            print("No data in this time round!. wait for the next round in 5 mins")
            time.sleep(300)
            continue
            
        for data in json_response['data']:
            tweet = data['text']
            tweet_date= data['created_at']
            uid = data['author_id']
            tweet_id = data['id']
            retweets= data['public_metrics']['retweet_count']
            impression_count= data['public_metrics']['impression_count']

            for user in includes['users']:
                if user['id'] == uid :
                    handle=user['username']
                    verified=user['verified']
                    screen_name=user['name']
                    followers = user['public_metrics']['followers_count']
                    if 'location' in user:
                        location=user['location']
                    else:
                        location='unknown'

            print("user " ,handle , '  '  )
            record = {
            "tweet_id": tweet_id,
            "tweet_date": tweet_date,
            "text": tweet,
            "retweets": retweets,
            "impression_count": impression_count,
            "username": handle,
            "user_id": uid,
            "verified":verified,    
            "followers": followers,
            "location": location}
            json_record = json.dumps(record)
            clientsocket.send((json_record+ "\n").encode('utf-8')) 

    time.sleep(300)
     
        

clientsocket.close()
