import logging
import requests
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
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
    print("*"*10, execution_date)

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
        ranking = boxof.find('rnum').text if boxof.find('rnum') is not None else None
        performance_id = boxof.find('mt20id').text if boxof.find('mt20id') is not None else None
        performance_name = boxof.find('prfnm').text if boxof.find('prfnm') is not None else None
        genre = boxof.find('cate').text if boxof.find('cate') is not None else None
        performance_count = boxof.find('prfdtcnt').text if boxof.find('prfdtcnt') is not None else None
        area = boxof.find('area').text if boxof.find('area') is not None else None

        result.append("('{}',{},'{}','{}','{}',{},'{}')".format(db_box_office_date, ranking, performance_id, performance_name, genre, performance_count, area))

    print(result)
    logging.info("extract and transform ended")
    return result

def load(**context):
    logging.info("load started")

    schema = context["params"]["schema"]
    table = context["params"]["table"]
    records = context["task_instance"].xcom_pull(key="return_value", task_ids="extract_transform")

    cur = get_Redshift_connection()

    # 원본 테이블이 없으면 생성
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            box_office_date date,
            ranking int,
            performance_id varchar(150),
            performance_name varchar(255),
            genre varchar(100),
            performance_count int,
            area varchar(100),
            created_at timestamp default GETDATE()
            );
    """
    logging.info(create_table_sql)

    # 임시 테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""
    logging.info(create_t_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 임시 테이블 데이터 입력
    insert_sql = f"INSERT INTO t VALUES " + ",".join(records)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 기존 테이블 대체
    alter_sql = f"""
        DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table}
        SELECT box_office_date, ranking, performance_id, performance_name, genre, performance_count, area FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY box_office_date, performance_id ORDER BY created_at DESC) seq
            FROM t
        )
        WHERE seq = 1;
    """
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")

dag = DAG(
    dag_id = 'Box_office_v2',
    start_date = datetime(2025,1,1),
    catchup=True,
    tags=['API'],
    schedule = '0 9 * * *' # 매일 12:10
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