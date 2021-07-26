import datetime
import json
from pymongo import MongoClient
import requests_oauthlib
import tqdm
import pandas as pd
from pyspark.sql import SparkSession


mongodb_host = "localhost"
mongodb_port = "27017"

mongo_client = MongoClient(mongodb_host, int(mongodb_port))
database = mongo_client.get_database('mydb')
collection = database.get_collection('twitter')
mongodb_uri = "mongodb://localhost/mydb.twitter"

def load_data():
    twitter = requests_oauthlib.OAuth1Session(consumer_key,consumer_secret,access_token_key,access_token_secret)
    uri='https://stream.twitter.com/1.1/statuses/sample.json'
    respone=twitter.get(uri,stream=True)

    if respone.status_code == '200':
        # mongodb connect

        for line in tqdm.tqdm(respone.iter_lines(),unit='tweets',mininterval=1):
            if line:
                tweet = json.loads(line)
                tweet['_timestamp']=datetime.datetime.utcnow().isoformat()
                # mongodb insert
                collection.insert_one(tweet)

def extract_data(*args, **kwargs):

    # mongodb extract
    for tweet in collection.find(*args, **kwargs):
        if 'delete' not in tweet:
            yield{
                'created_at':tweet['created_at'],'text':tweet['text']
            }

if __name__ == "__main__":
    load_data()
    df= pd.DataFrame(extract_data({'lang':'ko'},limit=10))
    print(df)
