# 버전2 : 버전1의 데이터무결성 문제를 트랜잭션으로 해결 

import requests
import psycopg2
import logging

# Redshift connection 함수
# - redshift_user와 redshift_pass를 본인 것으로 수정!
def get_Redshift_connection(autocommit):
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "gg66477"
    redshift_pass = "Gg66477!1"
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
    # BasicETL.py의 문제점 해결 : BEGIN과 END를 사용해서 SQL 결과를 트랜잭션 만듦!
    # BEGIN;DELETE FROM gg66477.name_gender;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');....;END;
    cur = get_Redshift_connection(True)
    sql = "BEGIN;DELETE FROM gg66477.name_gender;"
    print("load started")
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            #print(name, "-", gender)
            sql += "INSERT INTO gg66477.name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
    sql += "END;"
    cur.execute(sql)
    print("load ended")

link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
data = extract(link)
lines = transform(data)
load(lines)