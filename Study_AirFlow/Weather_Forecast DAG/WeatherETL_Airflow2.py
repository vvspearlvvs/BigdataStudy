import requests
import json
import logging
from datetime import datetime
from datetime import timedelta

# airflow추가
from airflow import DAG
from airflow.operators.python import PythonOperator

# 환경변수 추가
from airflow.models import Variable 

# Connection객체 추가
from airflow.hooks.postgres_hook import PostgresHook

def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

# 하나의 etl()에서 여러개 task수행
def etl(**context):
    
    #extract
    api_key = Variable.get("open_weather_api_key")     # APIKey 환경변수
    api_url = Variable.get("open_weather_api_url")     # APIURL 환경변수

    schema = context['params']['schema']               # DB스키마 파라미터
    table = context['params']['table']               # DB테이블 파라미터

    lat = context['params']['lat']               # 위도 파라미터
    lon = context['params']['lat']               # 경도 파라미터

    request_url = api_url+"lat=%s&lon=%s&appid=%s&units=metric" % (lat, lon, api_key)
    response = requests.get(request_url)
    data = json.loads(response.text)

    #transform
    result =[]
    for d in data['daily']:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        result.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))
    
    #load
    cur = get_Redshift_connection()

    #임시테이블 CREATE
    create_sql = "DROP TABLE IF EXISTS {schema}.{table}; \
                  CREATE TALBE {schema}.tmp_{table} AS SELECT * FROM {schema}.{table};"
    logging.info(create_sql)
    try:
        cur.execute(create_sql)
        logging.info("create table")

        cur.execute("Commit;")  # Connection객체를 사용할 경우 autocommit은 항상 False!!
    except Exception as e:
        cur.execute("Rollback;")
        raise 

    #임시테이블 데이터INSERT
    insert_sql = "INSERT INTO {schema}.temp_{table} VALUES " + ",".join(result)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise
    
    #기존테이블 대체 
    #임시테이블에서 update_date를 이용해 최신 날짜의 레코드를 찾고, 원본테이블에 insert 
    alter_sql = "DELETE FROM {schema}.{table};\
        INSERT INTO {schema}.{table}\
        SELECT date, temp, min_temp, max_temp FROM (\
            SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY updated_date DESC) seq\
            FROM {schema}.temp_{table} \
            ) \
        WHERE seq = 1;"
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise

openweather_dag = DAG(
    dag_id = 'Weather_to_Redshift_test2',
    start_date = datetime(2021,9,3), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 2 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

# task생성 : extract,tranform,load 각각 task
extract = PythonOperator(
    task_id = 'extract',
    python_callable = etl,
    params = {
        'seoul_lat' : 37.5665,      # 서울위도
        'seoul_lon' : 126.9780,     # 서울경도
        "schema": "gg66477",        # DB스키마
        "table": "weather_forecast" # DB테이블
    },
    provide_context=True,
    dag = openweather_dag)