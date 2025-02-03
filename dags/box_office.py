import logging
import requests
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract_transform(execution_date):
    logging.info("extract and transform started")

    box_office_url = Variable.get("box_office_url")
    service_key = Variable.get("service_key")
    box_office_date = (datetime.strptime(execution_date, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

    url = f"{box_office_url}?service={service_key}&stdate={box_office_date}&eddate={box_office_date}"
    response = requests.get(url)
    xml_data = response.text.strip()

    root = ET.fromstring(xml_data)
    db_box_office_date = datetime.strptime(box_office_date, '%Y%m%d')
    result = []

    for boxof in root.findall('boxof'):
        ranking = boxof.find('rnum').text if boxof.find('rnum') is not None else ""
        performance_id = boxof.find('mt20id').text if boxof.find('mt20id') is not None else ""
        performance_name = boxof.find('prfnm').text if boxof.find('prfnm') is not None else ""
        genre = boxof.find('cate').text if boxof.find('cate') is not None else ""
        performance_count = boxof.find('prfdtcnt').text if boxof.find('prfdtcnt') is not None else ""
        area = boxof.find('area').text if boxof.find('area') is not None else ""

        result.append([db_box_office_date, ranking, performance_id, performance_name, genre, performance_count, area])

    # 결과 출력
    print(result)
    logging.info("extract and transform ended")
    return result

def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                box_office_date date,
                ranking int,
                performance_id varchar(150),
                performance_name varchar(255),
                genre varchar(100),
                performance_count int,
                area varchar(100),
                created_at timestamp default GETDATE(),
            );
        """
    )

def load(**context):
    logging.info("load started")

    schema = context["params"]["schema"]
    table = context["params"]["table"]
    records = context["task_instance"].xcom_pull(key="return_value", task_ids="extract_transform")

    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성 - 테이블이 처음 한번 만들어질 때 필요한 코드
        _create_table(cur, schema, table, False)
        # 임시 테이블로 원본 테이블을 복사
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"INSERT INTO t VALUES ('{r[0]}', {r[1]}, '{r[2]}', '{r[3]}', '{r[4]}', {r[5]}, '{r[6]}');"
            print(sql)
            cur.execute(sql)

        # 원본 테이블 생성
        _create_table(cur, schema, table, True)
        # 임시 테이블 내용을 원본 테이블로 복사
        cur.execute(f"INSERT INTO {schema}.{table} SELECT DISTINCT * FROM t;")
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")

dag = DAG(
    dag_id = 'Box_office_v1',
    start_date = datetime(2025,2,1),
    catchup=True,
    tags=['API'],
    schedule = '10 9 * * *' # 매일 12:10
)


extract_transform = PythonOperator(
    task_id='extract_transform',
    python_callable=extract_transform,
    op_kwargs={
        "execution_date": "{{execution_date.in_timezone('Asia/Seoul').strftime('%Y%m%d')}}"
    },
    dag=dag)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params = {
        'schema': 'soojiin_leee',
        'table': 'box_office',
    },
    dag=dag)

extract_transform >> load