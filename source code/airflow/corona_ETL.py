##############
#DAG Setting
##############

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime


dag = DAG(
        dag_id = "corona",
        start_date = datetime(2021,10,3),
        schedule_interval = '@once'
    )


#############
#Python code
#############

# start
def print_start():
	print("ETL ready")
	return "ETL ready"


# corona open API
def API_call():    
    import pandas as pd
    from datetime import datetime
    import requests
    import xmltodict
    import time
    import re
    import warnings
    warnings.filterwarnings('ignore')

    # corona open api 가져오기      
    url_district_base = "http://openapi.data.go.kr/openapi/service/rest/Covid19/getCovid19SidoInfStateJson"
    url_district_serviceKey = "S8%2Ftx%2BhEP7bZDZI%2By0P1ZKvPuHpx%2BVUKpt6ay8faxnxR%2FTRO9M5UAy8%2BafhJBNVzQG%2Fgwoym2S4Xbe1dUXivUw%3D%3D"

    nowtime = datetime.today().strftime("%Y-%m-%d")
    url_pages = "1000" #페이지당열갯수
    url_start_date = datetime.today().strftime("%Y%m%d%H%M%S")[:8]#"20200303" #시작날짜
    url_end_date = datetime.today().strftime("%Y%m%d%H%M%S")[:8] #끝날짜

    # open api 가져오기
    url_district= url_district_base + "?serviceKey=" + url_district_serviceKey + "&pageNo=1&numOfRows=" + url_pages + "&startCreateDt="+ url_start_date + "&endCreateDt=" + url_end_date

    req = requests.get(url_district).content
    xmlObject = xmltodict.parse(req)
    dict_data = xmlObject['response']['body']['items']['item']

    # 전처리
    dfDistrict = pd.DataFrame(dict_data)

    # 합계, 검역 삭제 후 dataframe 내에서 index 다시 0부터 차례대로 지정
    dfDistrict = dfDistrict[(dfDistrict['gubun'] != '합계') & (dfDistrict['gubun'] != '검역')].reset_index(drop=True).copy()
    dfDistrict.drop(['stdDay', 'updateDt', 'seq', 'qurRate', 'gubunCn'], axis=1, inplace=True)
    dfDistrict = dfDistrict.astype({"createDt":"datetime64[ns]"}).copy()
    # dfDistrict['createDt'] = dfDistrict['createDt'] - pd.DateOffset(days=2)
    dfDistrict['stateDt'] = dfDistrict['createDt'].dt.date         # YYYY-MM-DD(문자)
    dfDistrict.drop(['createDt'], axis=1, inplace=True)
    dfDistrict.drop(['gubunEn'], axis=1, inplace=True)

    # type 변환
    dfDistrict = dfDistrict.astype({"stateDt":"datetime64[ns]", "defCnt":"int64", "deathCnt":"int64", "incDec":"int64"}).copy()
    dfDistrict = dfDistrict[['stateDt', 'gubun', 'defCnt', 'deathCnt', 'incDec', 'isolClearCnt', 'isolIngCnt', 'localOccCnt', 'overFlowCnt']].copy()

    # null값 처리
    dfDistrict['isolClearCnt'] = dfDistrict['isolClearCnt'].fillna(0).astype("int64")
    dfDistrict['isolIngCnt'] = dfDistrict['isolIngCnt'].fillna(0).astype("int64")
    dfDistrict['localOccCnt'] = dfDistrict['localOccCnt'].fillna(0).astype("int64")
    dfDistrict['overFlowCnt'] = dfDistrict['overFlowCnt'].fillna(0).astype("int64")

    # Dataframe 형태 정리
    dfDistrict = dfDistrict.astype({"isolClearCnt":"int64", "isolIngCnt":"int64", "localOccCnt":"int64", "overFlowCnt":"int64"}).copy()
    dfDistrict.rename(columns={'stateDt':'날짜', 'gubun':'Area', 'defCnt':'확진자수', 'deathCnt':'사망자수', 'incDec':'전일대비증감수',
                               'isolClearCnt':'격리해제수', 'isolIngCnt':'격리중환자수', 'localOccCnt':'지역발생수', 
                               'overFlowCnt':'해외유입수'}, inplace=True)

    dfDistrict.to_parquet(f"/home/ubuntu/DE/coronaAPI_{nowtime}")


# coroan 단계별 거리두기 크롤링
def stage_call():
    from datetime import datetime
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from pyvirtualdisplay import Display

    from datetime import datetime, timedelta
    from pymongo import MongoClient
    import pandas as pd 
    import numpy as np
    import json

    import subprocess
    display = Display(visible=0, size=(1024, 768)) 
    display.start()
 
    path = '/home/ubuntu/chromedriver' 
    driver = webdriver.Chrome(path)

    driver.get('http://ncov.mohw.go.kr/regSocdisBoardView.do')
    info_df = pd.DataFrame(columns=("Area","Stage","Description"))
    idx = 0

    for i in range(1,18):
        location = driver.find_element_by_xpath(f'//*[@id="main_maplayout"]/button[{i}]')
        location.click()

        area = driver.find_element_by_xpath(f'//*[@id="step_map_city{i}"]/h3').text
        stage =  driver.find_element_by_xpath(f'//*[@id="step_map_city{i}"]/h4').text
        description = driver.find_element_by_xpath(f'//*[@id="step_map_city{i}"]/p').text

        # 확인용
        area_info = [area,stage,description]
        info_df.loc[idx] = area_info
        idx += 1

        driver.implicitly_wait(3)
    
    nowtime = datetime.today().strftime("%Y-%m-%d")
    info_df.to_parquet(f'/home/ubuntu/DE/coronaStage_{nowtime}')


# corona open api : ubuntu -> hdfs로 이동
def coronaAPI_to_hdfs():
    import subprocess
    # move file to hdfs.
    nowtime = datetime.today().strftime("%Y-%m-%d")
    load_to_hadoop_script = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/coronaAPI_'+nowtime, '/data/corona')
    #logger.info("SPOT.Utils: Loading file to hdfs: {0}".format(load_to_hadoop_script))
    subprocess.call(load_to_hadoop_script,shell=True)


# corona Stage : ubuntu -> hdfs로 이동
def coronaStage_to_hdfs():
    import subprocess
    # move file to hdfs.
    nowtime = datetime.today().strftime("%Y-%m-%d")
    load_to_hadoop_script = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/coronaStage_'+nowtime, '/data/corona')
    #logger.info("SPOT.Utils: Loading file to hdfs: {0}".format(load_to_hadoop_script))
    subprocess.call(load_to_hadoop_script,shell=True)


# hdfs -> mongoDB 이동
def corona_processing():
    from pyspark.conf import SparkConf 
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    import pyspark
    import pyspark.sql.functions as f 

    from pymongo import MongoClient
    from datetime import datetime
    import json

    # pymongo connect
    client = MongoClient('localhost',27017) # mongodb 27017 port
    db = client.ojo_db

    spark = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
    nowtime = datetime.today().strftime('%Y-%m-%d')

    disp = spark.read.parquet(f"hdfs://localhost:9000/data/corona/coronaAPI_{nowtime}")
    disp_st = spark.read.parquet(f"hdfs://localhost:9000/data/corona/coronaStage_{nowtime}")

    df = disp.select('날짜','area','확진자수')
    df = df.withColumn('date',f.to_date(df['날짜']).cast(StringType()))
    df = df.drop('날짜')
    df = df.withColumnRenamed("date", "날짜")

    disp_st = disp_st.drop('__index_level_0__')
    disp_st = disp_st.withColumnRenamed("Stage", "거리두기단계")

    df_corona = df.join(disp_st, on=['Area'], how='left_outer')
    df_corona = df_corona.withColumnRenamed("area", "지역")
    df_corona = df_corona.withColumnRenamed("Description", "상세내용")
    
    for i in df_corona.collect():
        db.corona.insert_one(i.asDict())


####################
# Dag Task Setting
####################

# (0)
ETL_ready = PythonOperator(
	task_id = 'ETL_ready',
	#python_callable param points to the function you want to run 
	python_callable = print_start,
	#dag param points to the DAG that this task is a part of
	dag = dag
    )


# (1) corona API
coronaAPI = PythonOperator(
        task_id = 'coronaAPI',
        python_callable = API_call,       
        #provide_context=True,
        dag = dag
        )
        
coronaAPI_move = PythonOperator(
    task_id = 'coronaAPI_move',
    python_callable = coronaAPI_to_hdfs,
    dag = dag
    )


# (2) corona Stage
coronaStage = PythonOperator(
    task_id = 'coronaStage',
    python_callable = stage_call,
    dag = dag
    )

coronaStage_move = PythonOperator(
    task_id = 'coronaStage_move',
    python_callable = coronaStage_to_hdfs,
    dag = dag
    )


# (3) load to MongoDB
coronaToMongo = PythonOperator(
    task_id = 'coronaToMongo',
    python_callable = corona_processing,
    dag = dag
    )


#Assign the order of the tasks in our DAG
#coronaAPI >> coronaStage >> coronaAPI_move >> coronaStage_move >> coronaToMongo
ETL_ready >> [coronaStage, coronaAPI]
coronaAPI >> coronaAPI_move
coronaStage >> coronaStage_move
[coronaAPI_move, coronaStage_move] >> coronaToMongo



