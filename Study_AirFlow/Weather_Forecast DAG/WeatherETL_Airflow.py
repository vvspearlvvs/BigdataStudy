import requests
import json
import logging
from datetime import datetime
from datetime import timedelta

from airflow.models import Variable

def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def extract(**context):
    
    api_key = Variable.get("open_weather_api_key")     # APIKey 환경변수
    api_url = Variable.get("open_weather_api_url")     # APIKey 환경변수
    
    lat = context['params']['lat']               # 위도 파라미터
    lon = context['params']['lat']               # 경도 파라미터

    request_url = api_url+"lat=%s&lon=%s&appid=%s&units=metric" % (lat, lon, api_key)
    response = requests.get(request_url)
    data = json.loads(response.text)

    return data

def tranform(**context):
    data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract") #extract함수에서 return한 value를 받아옴

    result =[]
    for d in data['daily']:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        result.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))

    return result

def load(**context):
    schema = "gg66477"
    table = "weather_forecast"
    cur = get_Redshift_connection()
    result = context["task_instance"].xcom_pull(key="return_value",task_id="transform") #transform함수에서 return한 value를 받아옴

    init_table_sql ="DROP TABLE IF EXISTS {schema}.{table};\
                    CREATE TABLE {schema}.{table} (\
                    date date,\
                    temp float,\
                    min_temp float,\
                    max_temp float,\
                    updated_date timestamp default GETDATE());".join(result).format(schema=schema, table=table)

    insert_data_sql = """DELETE FROM {schema}.{table};INSERT INTO {schema}.{table} VALUES """ + ",".join(result)
    insert_data_sql = insert_data_sql.format(schema=schema, table=table)
    
    try:
        cur.execute(init_table_sql)
        logging.info("create table")

        cur.execute(insert_data_sql)
        logging.info("insert data")

        cur.execute("Commit;")  # Connection객체를 사용할 경우 autocommit은 항상 False!!
    except Exception as e:
        cur.execute("Rollback;")
   
    
openweather_dag = DAG(
    dag_id = 'Weather_to_Redshift_test',
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
    python_callable = extract,
    params = {
        'seoul_lat' : 37.5665,  # 서울위도
        'seoul_lon' : 126.9780  # 서울경도
    },
    provide_context=True,
    dag = openweather_dag)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    provide_context=True,
    dag = openweather_dag)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'gg66477',
        'table': 'weather_forecast'
    },
    provide_context=True,
    dag = openweather_dag)

# task의 순서
extract >> transform >> load
