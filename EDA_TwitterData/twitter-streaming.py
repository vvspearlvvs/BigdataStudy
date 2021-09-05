import datetime
import json
from pymongo import MongoClient
import requests_oauthlib
import tqdm
import pandas as pd
from pyspark.sql import SparkSession


consumer_key = 'm4NgxpjnRFBhOtHYqHCt37pnW'
consumer_secret='IIyzsZxpptxJXvJKUIxfmAvkJ0DDkAY9GrfXH6dvRDZOcICUiL'
access_token_key='1416432618233503746-rPj5iB4SNczL3tGlcCV9BSrmlyLFNN'
access_token_secret='iX7D5SNM2lQogUSG4lxNmMaG2rh8Yw8d2BTKfJhNhv6eL'

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
                print("raw data")
                print(tweet)
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

def spark_session(mongodb_uri):
    # spark transform
    spark = SparkSession.builder.appName("myapp") \
        .config("spark.mongodb.input.uri",mongodb_uri) \
        .config("spark.mongodb.ouput.uri",mongodb_uri) \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    return spark

    #df.createTempView('tweets')
    #df.printSchema()

if __name__ == "__main__":
    load_data()
    df= pd.DataFrame(extract_data({'lang':'ko'},limit=10))
    print(df)

    print("spark session")
    spark = spark_session(mongodb_uri)
    df=spark.read.format("com.mongodb.spark.sql.DefaultSource").load().show(10,False)
    print(df)

    #문제 : extract한 data를 spark session에서 읽어야하는데 현재 mongodb에 있는 rawdata를 가지고 오는 중..
