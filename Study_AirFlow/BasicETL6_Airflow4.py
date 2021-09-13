# 버전6 수정사항
# 1. Connection객체를 사용해서 redshift 접속연결 
 

import requests
import psycopg2
import logging
from datetime import datetime

# airflow추가
from airflow import DAG
from airflow.operators.python import PythonOperator

# 환경변수 추가
from airflow.models import Variable 

# Connection객체 추가
from airflow.hooks.postgres_hook import PostgresHook

# Redshift connection 함수 -> Connection객체사용
# - redshift_user와 redshift_pass를 본인 것으로 수정!
def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    '''
    Airflow WEB-Admin-Connections에서 직접 Connection 설정
        Conn id : redshift_dev_db
        Conn Type : Postgres
        Host : learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com
        Schema(dbname) : dev 
        Login : redshift_user
        password : redshift_pass
        port : 5439
    '''
    return hook.get_conn().cursor()

def extract(**context):
    link = context["params"]["url"] # S3 url 파라미터
    task_instance = context['task_instance']
    execution_date = context['execution_date'] 
    
    logging.info(execution_date)

    f = requests.get(link) # s3에 있는 csv 데이터 가져옴
    print("Extract ended")

    return (f.text)

def transform(**context):
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract") #extract함수에서 return한 value를 받아옴
    lines = text.split("\n")[1:] # 맨 윗줄 제거하도록 변형
    print("transform ended")

    return lines

def load(**context):
    schema = context["params"]["schema"]    # DB 스키마 파라미터
    table = context["params"]["table"]      # DB 테이블 파라미터

    cur = get_Redshift_connection()         # Connection객체를 사용할 경우 autocommit은 항상 False!!
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform") #transform함수에서 return한 value를 받아옴
    lines = iter(lines)
    next(lines)

    sql = "DELETE FROM {schema}.{table};".format(schema=schema, table=table)
    print("load started")
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            #print(name, "-", gender)
            sql += """INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');""".format(schema=schema, table=table, name=name, gender=gender)
    sql += "Commit;"
    cur.execute(sql)
    print("load ended")



# DAG설정 
dag_second_assignment = DAG(
	dag_id = 'second_assignment_v3', # DAG이름
    catchup = False,                 # Backfill수행하지 않음 : 과거의 내용은 무시하고 미래만 실행
	start_date = datetime(2021,9,3), # 날짜가 미래인 경우 실행이 안됨
	schedule_interval = '0 2 * * *', # 스케쥴링시간(ex:매일 오전2시)
    default_args = {                 # task실패시 retry할 옵션
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'max_active_runs': 1
        }
    
    ) 
     
# task생성 : extract,tranform,load 각각 task
extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url':  Variable.get("csv_url") #환경변수설정
    },
    provide_context=True,
    dag = dag_second_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    provide_context=True,
    dag = dag_second_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'gg66477',
        'table': 'name_gender'
    },
    provide_context=True,
    dag = dag_second_assignment)

# task의 순서
extract >> transform >> load