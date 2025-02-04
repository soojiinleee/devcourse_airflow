# 목표 : extract한 데이터 S3에 업로드
# 1. extract : xml 데이터 S3에 바로 업로드 (공연 목록)
# 1-1. extract : S3에 저장된 xml 공연 목록에서 공연 ID 확인 후 공연 상세 정보 바로 S3에 업로드
# 2. transform : S3에 어
# 2. transform : S3에 업로드된
# 3. load -> S3

import requests
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from lxml import etree


# 저장 경로 설정 (execution_date 날짜로 수정)
OUTPUT_FILE = f"/opt/airflow/data/{datetime.date()}.parquet"
BASE_URL = Variable.get("performance_url")
SERVICE_KEY = Variable.get("service_key")
PAGE_SIZE = 100

def get_performance_detail(performance_id):
    """ 공연 상세 API 수집"""
    response = requests.get(f"{BASE_URL}/{performance_id}?service={SERVICE_KEY}")
    response_data = etree.fromstring(response)

    if response_data.find("dbs") is None:
        raise logging.info(f"공연 상세 API 호출 실패 >> performance_id : {performance_id}")

    # 필요한 데이터 추출
    for performance in response_data.xpath("//db"):
        styurls = [styurl.text for styurl in performance.xpath(".//styurls/styurl")]
        related_urls = [
            {
                "relatenm": relate.findtext("relatenm"),
                "relateurl": relate.findtext("relateurl")
            }
            for relate in performance.xpath(".//relate")
        ]

        performance_detail = {
            "mt20id": performance.findtext("mt20id"),
            "prfnm": performance.findtext("prfnm"),
            "prfpdfrom": performance.findtext("prfpdfrom"),
            "prfpdto": performance.findtext("prfpdto"),
            "fcltynm": performance.findtext("fcltynm"),
            "prfcast": performance.findtext("prfcast"),
            "prfruntime": performance.findtext("prfruntime"),
            "prfage": performance.findtext("prfage"),
            "pcseguidance": performance.findtext("pcseguidance"),
            "poster": performance.findtext("poster"),
            "area": performance.findtext("area"),
            "genrenm": performance.findtext("genrenm"),
            "openrun": performance.findtext("openrun"),
            "prfstate": performance.findtext("prfstate"),
            "styurl": styurls,
            "related_urls": related_urls
        }

        return performance_detail

def extract_transform_performance_data(**kwargs):
    """ main 함수 처럼 작성하기"""

    current_page = 1
    start_date = 20250201 # execution_date 으로 수정 필요
    end_date = 20250201 # next_execution_date -1 으로 수정 필요

    all_performance_details = []    # 전체 공연 상세 데이터

    while True:
        # 공연 목록 조회
        response = requests.get(f"{BASE_URL}?service={SERVICE_KEY}&stdate={start_date}&eddate={end_date}&cpage={current_page}&rows={PAGE_SIZE}")
        response_data = etree.fromstring(response)

        # 공연 목록 데이터 없으면 종료
        if response_data.find("dbs") is None:
            break

        # 공연 ID 추출
        performance_id_list = [element.text for element in response_data.xpath("//mt20id")]

        for performance_id in performance_id_list:
            # 각 공연 상세 조회
            performance_detail = get_performance_detail(performance_id)
            all_performance_details.append(performance_detail)


        current_page += 1   # 다음 페이지로 이동

    # 전체 공연 상세 데이터 -> Pandas DataFrame으로 변환
    df = pd.DataFrame(all_performance_details)
    return df

    # 수집된 데이터를 파일로 저장
    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(all_performance_details, file, ensure_ascii=False, indent=4)

# Airflow DAG 정의
default_args = {
    "owner": "Soojin Lee",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "performance_data_extraction_transformation",
    default_args=default_args,
    description="KOPIS 공연 상세 데이터 S3 업로드 작업",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract_transform_performance_data = PythonOperator(
        task_id="extract_transform_performance_data",
        python_callable=extract_transform_performance_data,
        provide_context=True,
    )

    extract_transform_performance_data
