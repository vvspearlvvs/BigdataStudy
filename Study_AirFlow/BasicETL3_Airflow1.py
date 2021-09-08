# 버전3 : s3에 있는 csv데이터 extract, transform , Redshift Load -> Airflow DAG생성

import requests
import psycopg2
import logging
from datetime import datetime

# airflow추가
from airflow import DAG
from airflow.operators.python import PythonOperator

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

def etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)


# DAG설정 
dag_second_assignment = DAG(
	dag_id = 'second_assignment',       # DAG이름
    catchup = False,                    # Backfill수행하지 않음 : 과거의 내용은 무시하고 미래만 실행
	start_date = datetime(2021,9,3),    # 날짜가 미래인 경우 실행이 안됨
	schedule_interval = '0 2 * * *')    # 스케쥴링시간(ex:매일 오전2시)

# task생성
task = PythonOperator(
	task_id = 'perform_etl',         # task이름
	python_callable = etl,           # 실제 파이프라인을 수행하는 python함수 etl()
    dag = dag_second_assignment)    # DAG 설정내용 
