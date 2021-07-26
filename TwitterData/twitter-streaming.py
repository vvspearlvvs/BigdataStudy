import datetime
import json
from pymongo import MongoClient
import requests_oauthlib
import tqdm



mongodb_host = "localhost"
mongodb_port = "27017"

def main():
    twitter = requests_oauthlib.OAuth1Session(consumer_key,consumer_secret,access_token_key,access_token_secret)
    uri='https://stream.twitter.com/1.1/statuses/sample.json'
    respone=twitter.get(uri,stream=True)
    print(respone.status_code)
    #print(r.raise_for_status().code())

    mongo_client = MongoClient(mongodb_host, int(mongodb_port))
    database = mongo_client.get_database('mydb')
    collection = database.get_collection('twitter')

    print(respone.iter_lines())
    for line in tqdm.tqdm(respone.iter_lines(),unit='tweets',mininterval=1):
        if line:
            tweet = json.loads(line)
            print(tweet)
            #tweet['_timestamp']=datetime.datetime.utcnow().isoformat()
            #collection.insert_one(tweet)


if __name__ == "__main__":
    main()