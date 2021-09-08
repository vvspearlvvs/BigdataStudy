# 버전1 : s3에 있는 csv데이터 extract, transform , Redshift Load

import requests
import psycopg2
import logging

# Redshift connection 함수
# - redshift_user와 redshift_pass를 본인 것으로 수정!
def get_Redshift_connection(autocommit):
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "gg66477"
    redshift_pass = ""
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=autocommit)
    return conn.cursor()

def extract(url):
    f = requests.get(url) # s3에 있는 csv 데이터 가져옴
    print("Extract ended")

    return (f.text)

def transform(text):
    lines = text.split("\n")[1:] # 맨 윗줄 제거하도록 변형
    print("transform ended")

    return lines

def load(lines):
    # 문제점 : refresh data를 하면서 데이터 무결성을 보장하지 못함 
    # 계속 데이터를 refresh하기 위해 delete , insert 반복 -> 그러다가 데이터중복이 발생할 수도..!
    cur = get_Redshift_connection(True)
    print("load started")
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            #print(name, "-", gender)
            sql = "INSERT INTO gg66477.name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
            cur.execute(sql)
    print("load ended")

link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
data = extract(link)
lines = transform(data)
load(lines)