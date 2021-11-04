##############
#DAG Setting
##############

import pandas as pd
import numpy as np
import re
import time
from kiwipiepy import Kiwi
# import tomotopy as tp
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import array_contains, udf
from datetime import datetime
# Word2Vec embedding
from gensim.models import Word2Vec
from numpy import dot
from numpy.linalg import norm
import numpy as np
from sklearn.cluster import KMeans
# from textrank import KeywordSummarizer
from scipy.sparse import csr_matrix
# from soyclustering import SphericalKMeans

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

dag = DAG(
        dag_id = "insta_fin",
        start_date = datetime(2021,10,2),
        schedule_interval = '@once'
    )

#############
#Python code
#############

def insta_call():
    import pandas as pd
    import subprocess
    from datetime import datetime

    import subprocess

    # 현재 시간 설정
    nowtime = datetime.today().strftime("%Y-%m-%d")


    # daily_gamsung (1)
    daily_gamsung = pd.read_csv(f'/home/ubuntu/DE/instagram crawling to csv/+URL/daily_gamsung_{nowtime}.csv')
    daily_gamsung_DF = pd.DataFrame(daily_gamsung)
    daily_gamsung_DF.drop('Unnamed: 0', axis=1, inplace=True)
    daily_gamsung_DF.to_parquet(f'/home/ubuntu/DE/daily_gamsung_{nowtime}.parquet')

    # gamsung_bnb (2)
    gamsung_bnb = pd.read_csv(f'/home/ubuntu/DE/instagram crawling to csv/+URL/gamsung.bnb_{nowtime}.csv')
    gamsung_bnb_DF = pd.DataFrame(gamsung_bnb)
    gamsung_bnb_DF.drop('Unnamed: 0', axis=1, inplace=True)
    gamsung_bnb_DF.to_parquet(f'/home/ubuntu/DE/gamsung_bnb_{nowtime}.parquet')

    # gamsung_curation (3)
    gamsung_curation = pd.read_csv(f'/home/ubuntu/DE/instagram crawling to csv/+URL/gamsung_curation_{nowtime}.csv')
    gamsung_curation_DF = pd.DataFrame(gamsung_curation)
    gamsung_curation_DF.drop('Unnamed: 0', axis=1, inplace=True)
    gamsung_curation_DF.to_parquet(f'/home/ubuntu/DE/gamsung_curation_{nowtime}.parquet')

    # hi.stay.tour (4)
    hi_stay_tour = pd.read_csv(f'/home/ubuntu/DE/instagram crawling to csv/+URL/hi.stay.tour_{nowtime}.csv')
    hi_stay_tour_DF = pd.DataFrame(hi_stay_tour)
    hi_stay_tour_DF.drop('Unnamed: 0', axis=1, inplace=True)
    hi_stay_tour_DF.to_parquet(f'/home/ubuntu/DE/hi_stay_tour_{nowtime}.parquet')


    # rest_behappyhere (5)
    rest_behappyhere = pd.read_csv(f'/home/ubuntu/DE/instagram crawling to csv/+URL/rest_behappyhere_{nowtime}.csv')
    rest_behappyhere_DF = pd.DataFrame(rest_behappyhere)
    rest_behappyhere_DF.drop('Unnamed: 0', axis=1, inplace=True)
    rest_behappyhere_DF.to_parquet(f'/home/ubuntu/DE/rest_behappyhere_{nowtime}.parquet')

    # sookso.diary (6)
    sookso_diary = pd.read_csv(f'/home/ubuntu/DE/instagram crawling to csv/+URL/sookso.diary_{nowtime}.csv')
    sookso_diary_DF = pd.DataFrame(sookso_diary)
    sookso_diary_DF.drop('Unnamed: 0', axis=1, inplace=True)
    sookso_diary_DF.to_parquet(f'/home/ubuntu/DE/sookso_diary_{nowtime}.parquet')

    # sookso.hada (7)
    sookso_hada = pd.read_csv(f'/home/ubuntu/DE/instagram crawling to csv/+URL/sookso.hada_{nowtime}.csv')
    sookso_hada_DF = pd.DataFrame(sookso_hada)
    sookso_hada_DF.drop('Unnamed: 0', axis=1, inplace=True)
    sookso_hada_DF.to_parquet(f'/home/ubuntu/DE/sookso_hada_{nowtime}.parquet')

    # sukso_name = [daily_gamsung, gamsung_bnb, gamsung_curation, hi_stay_tour, rest_behappyhere, sookso_diary, sookso_hada]

    # for sukso in sukso_name:
    #     load_to_hadoop_script = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/sukso_'+nowtime+ '.parquet', '/data/insta')
    
    # subprocess.call(load_to_hadoop_script,shell=True)




def daily_gamsung_extract(x):
    import re
    import numpy as np
    p = re.compile('\"[가-힣\t\n\r\f\v\s\_가-힣]+\"')
    m = p.findall(x)
    try:
        return m[0][1:-1]
    except:
        return np.nan

def gamsung_bnb_extract(x):
    import re
    import numpy as np
    p = re.compile("[\'|\"][가-힣]+[\'|\"]") 
    m = p.findall(x)
    try:
        return m[0][1:-1]
    except :
        return np.nan

def gamsung_curation_extract(x):
    import re
    import numpy as np
    p = re.compile('\📍[가-힣]+') # 
    m = p.findall(x)
    try:
        return m[0][1:] 
    except :
        return ''

def hi_stay_tour_extract(x):
    import re
    import numpy as np
    p = re.compile("\#[가-힣]+") # 
    m = p.findall(x)
    try:
        return m[0][1:] 
    except :
        return ''

def rest_behappyhere_extract(x):
    import re
    import numpy as np
    p = re.compile('\#[가-힣]+')
    m = p.findall(x)
    try:
        return m[1][1:]
    except:
        return np.nan

def sookso_diary_extract(x):
    import re
    import numpy as np
    p = re.compile('\📍 [가-힣]+\s[가-힇]+|\📍[가-힣]+\s[가-힇]+')
    m = p.findall(x)
    try:
        return m[0].split(' ')[-1]
    except:
        return np.nan

def sookso_hada_extract(x):
    import re
    import numpy as np
    p = re.compile('\#[가-힣]+')
    m = p.findall(x)
    if  m[1] == '#숙소하다': m[1], m[0] = m[0], m[1]
    return m[1][1:]


def daily_gamsung_extract(x):
    import re
    import numpy as np
    p = re.compile('\"[가-힣\t\n\r\f\v\s\_가-힣]+\"')
    m = p.findall(x)
    try:
        return m[0][1:-1]
    except:
        return np.nan

# def convert(x):
#     return int(x.replace(',',''))

def convert(x):
    if x:
        return int(x.replace(',',''))
    else:
        return 0

#####################################################
### daily_gamsung(완료)    
def insta_spark_processing():
    from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    from pyspark.sql.functions import array_contains, udf
    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession


    from datetime import datetime, timedelta
    from pymongo import MongoClient
    import pandas as pd
    import numpy as np
    from difflib import SequenceMatcher
    from collections import Counter
    from datetime import datetime
    import re

    import time
    import os
    import sys
    import urllib.request
    import json

    nowtime = datetime.today().strftime("%Y-%m-%d")#  - pd.DateOffset(days=1)
    spark = SparkSession\
            .builder\
            .appName('Python Spark SQL basic example')\
            .getOrCreate()

    sc = spark.sparkContext
    
    # pymongo connect
    client = MongoClient('localhost',27017) # mongodb 27017 port
    db = client.ojo_db
    cur = db['cluster']

    coronaStage = spark.read.parquet(f"hdfs://localhost:9000/data/corona/coronaStage_2021-10-04")

    daily_gamsung_DFP = spark.read.parquet(f"hdfs://localhost:9000/data/insta/daily_gamsung_{nowtime}.parquet")
    daily_gamsung_DFP = daily_gamsung_DFP.drop('Unnamed: 0','unix')
    daily_gamsung = daily_gamsung_DFP.toPandas()

    gamsung_bnb_DFP = spark.read.parquet(f"hdfs://localhost:9000/data/insta/gamsung_bnb_{nowtime}.parquet")
    gamsung_bnb_DFP = gamsung_bnb_DFP.drop('Unnamed: 0','unix')
    gamsung_bnb = gamsung_bnb_DFP.toPandas()

    gamsung_curation_DFP = spark.read.parquet(f"hdfs://localhost:9000/data/insta/gamsung_curation_{nowtime}.parquet")
    gamsung_curation_DFP = gamsung_curation_DFP.drop('Unnamed: 0','unix')
    gamsung_curation = gamsung_curation_DFP.toPandas()

    hi_stay_tour_DFP = spark.read.parquet(f"hdfs://localhost:9000/data/insta/hi_stay_tour_{nowtime}.parquet")
    hi_stay_tour_DFP = hi_stay_tour_DFP.drop('Unnamed: 0','unix')
    hi_stay_tour = hi_stay_tour_DFP.toPandas()

    rest_behappyhere_DFP = spark.read.parquet(f"hdfs://localhost:9000/data/insta/rest_behappyhere_{nowtime}.parquet")
    rest_behappyhere_DFP = rest_behappyhere_DFP.drop('Unnamed: 0','unix')
    rest_behappyhere = rest_behappyhere_DFP.toPandas()

    sookso_diary_DFP = spark.read.parquet(f"hdfs://localhost:9000/data/insta/sookso_diary_{nowtime}.parquet")
    sookso_diary_DFP = sookso_diary_DFP.drop('Unnamed: 0','unix')
    sookso_diary = sookso_diary_DFP.toPandas()

    sookso_hada_DFP = spark.read.parquet(f"hdfs://localhost:9000/data/insta/sookso_hada_{nowtime}.parquet")
    sookso_hada_DFP = sookso_hada_DFP.drop('Unnamed: 0','unix')
    sookso_hada = sookso_hada_DFP.toPandas()
    
    #######################################
    daily_gamsung_name = daily_gamsung.content.apply(lambda x : daily_gamsung_extract(x))
    daily_gamsung['name'] = daily_gamsung_name
    daily_gamsung = daily_gamsung[~(daily_gamsung['name'].isna())]

    # name data가 없는 index 지우기
    indexNames = daily_gamsung[daily_gamsung['name'] == ''].index
    daily_gamsung.drop(indexNames, inplace=True) 

    if daily_gamsung.like.dtype == 'O':
        daily_gamsung.like = daily_gamsung.like.apply(lambda x : convert(x))


    # 1,2,3
    dg_cnt = Counter(daily_gamsung.name)
    li =[]
    for name, cnt  in dg_cnt.items():
        if cnt > 1:
            li.append(name)   

    # 4. 행가져와서 like집계, content, tag list append
    for i in li:           # 중복된 name 반복
        ex = daily_gamsung.loc[daily_gamsung.name == i]  # 반복된 name의 행을 가져온다
        ex.reset_index(drop=True,inplace=True)     # index reset

        content = ex.iloc[0,:].content
        date = ex.iloc[0,:].date
        like = ex.iloc[0,:].like
        place = ex.iloc[0,:].place
        tags = ex.iloc[0,:].tags
        imgUrl = ex.iloc[0,:].imgUrl
        name = ex.name[0]

        for i in range(len(ex)-1):
            like += ex.iloc[i+1,:].like
            tags += ex.iloc[i+1,:].tags

        # 5.차례대로 기존 행 지워내기
        indexNames = daily_gamsung[daily_gamsung['name'] == ex.name[0]].index
        daily_gamsung.drop(indexNames, inplace=True) 
        daily_gamsung = daily_gamsung.append(pd.Series(data=[content,date,like,place,tags,imgUrl,name],index=daily_gamsung.columns),ignore_index=True)

    dg_tmp = []
    for i in daily_gamsung.name:
        dg_tmp.append(dg_cnt[i]-1)

    daily_gamsung['overlap'] = dg_tmp


    #####################################################
    ### gamsun.bnb
    gamsung_bnb_name = gamsung_bnb.content.apply(lambda x : gamsung_bnb_extract(x)) # gamsung_bnb_name = 
    gamsung_bnb['name'] = gamsung_bnb_name
    gamsung_bnb = gamsung_bnb[~(gamsung_bnb['name'].isna())]

    # name data가 없는 index 지우기
    indexNames = gamsung_bnb[gamsung_bnb['name'] == ''].index
    gamsung_bnb.drop(indexNames, inplace=True) 

    # like integet 변환 후 합치기

    if gamsung_bnb.like.dtype == 'O':
        gamsung_bnb.like = gamsung_bnb.like.apply(lambda x : convert(x))



    # 1, 중복된 이름 검색 
    # 2.개수가 1인거 이상 뽑아내기 
    # 3.리스트만들기
    gb_cnt = Counter(gamsung_bnb.name)      # Counter 모듈로 name data count
    li =[]
    for name, cnt  in gb_cnt.items():            # 중복된 것만 뽑기 (cnt가 1이상이면 중복)
        if cnt > 1:
            li.append(name)

    # 4. 행가져와서 like집계, content, tag list append
    for i in li:           # 중복된 name 반복
        ex = gamsung_bnb.loc[gamsung_bnb.name == i]  # 반복된 name의 행을 가져온다
        ex.reset_index(drop=True,inplace=True)     # index reset

        content = ex.iloc[0,:].content
        date = ex.iloc[0,:].date
        like = ex.iloc[0,:].like
        place = ex.iloc[0,:].place
        tags = ex.iloc[0,:].tags
        imgUrl = ex.iloc[0,:].imgUrl
        name = ex.name[0]

        for i in range(len(ex)-1):
            like += ex.iloc[i+1,:].like
            tags += ex.iloc[i+1,:].tags


        # 5.차례대로 기존 행 지워내기
        indexNames = gamsung_bnb[gamsung_bnb['name'] == ex.name[0]].index
        gamsung_bnb.drop(indexNames, inplace=True) 
        gamsung_bnb = gamsung_bnb.append(pd.Series(data=[content,date,like,place,tags,imgUrl,name],index=gamsung_bnb.columns),ignore_index=True)

    gb_tmp = []
    for i in gamsung_bnb.name:
        gb_tmp.append(gb_cnt[i]-1)


    gamsung_bnb['overlap'] = gb_tmp


    #####################################################
    ### gamsung_curation(완료)
    gamsung_curation_name = gamsung_curation.content.apply(lambda x : gamsung_curation_extract(x))
    gamsung_curation['name'] = gamsung_curation_name
    gamsung_curation = gamsung_curation[~(gamsung_curation['name'].isna())]


    # name data가 없는 index 지우기
    indexNames = gamsung_curation[gamsung_curation['name'] == ''].index
    gamsung_curation.drop(indexNames, inplace=True) 




    if gamsung_curation.like.dtype == 'O':
        gamsung_curation.like = gamsung_curation.like.apply(lambda x : convert(x))


    gc_cnt = Counter(gamsung_curation.name)      # Counter 모듈로 name data count
    li =[]
    for name, cnt  in gc_cnt.items():            # 중복된 것만 뽑기 (cnt가 1이상이면 중복)
        if cnt > 1:
            li.append(name)        


    # 4. 행가져와서 like집계, content, tag list append
    for i in li:           # 중복된 name 반복
        ex = gamsung_curation.loc[gamsung_curation.name == i]  # 반복된 name의 행을 가져온다
        ex.reset_index(drop=True,inplace=True)     # index reset

        content = ex.iloc[0,:].content
        date = ex.iloc[0,:].date
        like = ex.iloc[0,:].like
        place = ex.iloc[0,:].place
        tags = ex.iloc[0,:].tags
        imgUrl = ex.iloc[0,:].imgUrl
        name = ex.name[0]

        for i in range(len(ex)-1):
            like += ex.iloc[i+1,:].like
            tags += ex.iloc[i+1,:].tags

        # 5.차례대로 기존 행 지워내기
        indexNames = gamsung_curation[gamsung_curation['name'] == ex.name[0]].index
        gamsung_curation.drop(indexNames, inplace=True) 
        gamsung_curation = gamsung_curation.append(pd.Series(data=[content,date,like,place,tags,imgUrl,name],index=gamsung_curation.columns),ignore_index=True)

    gc_tmp = []
    for i in gamsung_curation.name:
        gc_tmp.append(gc_cnt[i]-1)

    gamsung_curation['overlap'] = gc_tmp

    #####################################################
    ### hi.stay.tour.csv (완료)
    hi_stay_tour_name = hi_stay_tour.content.apply(lambda x : hi_stay_tour_extract(x))
    hi_stay_tour['name'] = hi_stay_tour_name
    hi_stay_tour = hi_stay_tour[~(hi_stay_tour['name'].isna())]


    # name data가 없는 index 지우기
    indexNames = hi_stay_tour[hi_stay_tour['name'] == ''].index
    hi_stay_tour.drop(indexNames, inplace=True) 


    if hi_stay_tour.like.dtype == 'O':
        hi_stay_tour.like = hi_stay_tour.like.apply(lambda x : convert(x))


    # 1, 중복된 이름 검색 
    # 2.개수가 1인거 이상 뽑아내기 
    # 3.리스트만들기
    hst_cnt = Counter(hi_stay_tour.name)      # Counter 모듈로 name data count
    li =[]
    for name, cnt  in hst_cnt.items():            # 중복된 것만 뽑기 (cnt가 1이상이면 중복)
        if cnt > 1:
            li.append(name)  

    # 4. 행가져와서 like집계, content, tag list append
    for i in li:           # 중복된 name 반복
        ex = hi_stay_tour.loc[hi_stay_tour.name == i]  # 반복된 name의 행을 가져온다
        ex.reset_index(drop=True,inplace=True)     # index reset

        content = ex.iloc[0,:].content
        date = ex.iloc[0,:].date
        like = ex.iloc[0,:].like
        place = ex.iloc[0,:].place
        tags = ex.iloc[0,:].tags
        imgUrl = ex.iloc[0,:].imgUrl
        name = ex.name[0]

        for i in range(len(ex)-1):
            like += ex.iloc[i+1,:].like
            tags += ex.iloc[i+1,:].tags



        # 5.차례대로 기존 행 지워내기
        indexNames = hi_stay_tour[hi_stay_tour['name'] == ex.name[0]].index
        hi_stay_tour.drop(indexNames, inplace=True) 
        hi_stay_tour = hi_stay_tour.append(pd.Series(data=[content,date,like,place,tags,imgUrl,name],index=hi_stay_tour.columns),ignore_index=True)

    hst_cnt_tmp = []
    for i in hi_stay_tour.name:
        hst_cnt_tmp.append(hst_cnt[i]-1)

    hi_stay_tour['overlap'] = hst_cnt_tmp


    #####################################################
    ### rest_behappyhere.csv
    rest_behappyhere_name = rest_behappyhere.content.apply(lambda x : rest_behappyhere_extract(x))
    rest_behappyhere['name'] = rest_behappyhere_name
    rest_behappyhere = rest_behappyhere[~(rest_behappyhere['name'].isna())]

    # name data가 없는 index 지우기
    indexNames = rest_behappyhere[rest_behappyhere['name'] == ''].index
    rest_behappyhere.drop(indexNames, inplace=True) 



    if rest_behappyhere.like.dtype == 'O':
        rest_behappyhere.like = rest_behappyhere.like.apply(lambda x : convert(x))


    # 1, 중복된 이름 검색 
    # 2.개수가 1인거 이상 뽑아내기 
    # 3.리스트만들기
    rb_cnt = Counter(rest_behappyhere.name)      # Counter 모듈로 name data count
    li =[]
    for name, cnt  in rb_cnt.items():            # 중복된 것만 뽑기 (cnt가 1이상이면 중복)
        if cnt > 1:
            li.append(name)


    #####################################################
    # 4. 행가져와서 like집계, content, tag list append
    for i in li:           # 중복된 name 반복
        ex = rest_behappyhere.loc[rest_behappyhere.name == i]  # 반복된 name의 행을 가져온다
        ex.reset_index(drop=True,inplace=True)     # index reset

        content = ex.iloc[0,:].content
        date = ex.iloc[0,:].date
        like = ex.iloc[0,:].like
        place = ex.iloc[0,:].place
        tags = ex.iloc[0,:].tags
        imgUrl = ex.iloc[0,:].imgUrl
        name = ex.name[0]

        for i in range(len(ex)-1):
            like += ex.iloc[i+1,:].like
            tags += ex.iloc[i+1,:].tags



        # 5.차례대로 기존 행 지워내기
        indexNames = rest_behappyhere[rest_behappyhere['name'] == ex.name[0]].index
        rest_behappyhere.drop(indexNames, inplace=True) 
        rest_behappyhere = rest_behappyhere.append(pd.Series(data=[content,date,like,place,tags,imgUrl,name],index=rest_behappyhere.columns),ignore_index=True)

    rb_cnt_tmp = []
    for i in rest_behappyhere.name:
        rb_cnt_tmp.append(rb_cnt[i]-1)

    rest_behappyhere['overlap'] = rb_cnt_tmp

    #####################################################
    ### sookso.diary(완료)
    sookso_diary_name = sookso_diary.content.apply(lambda x : sookso_diary_extract(x))
    sookso_diary['name'] = sookso_diary_name
    sookso_diary = sookso_diary[~(sookso_diary['name'].isna())]


    # name data가 없는 index 지우기
    indexNames = sookso_diary[sookso_diary['name'] == ''].index
    sookso_diary.drop(indexNames, inplace=True) 



    if sookso_diary.like.dtype == 'O':
        sookso_diary.like = sookso_diary.like.apply(lambda x : convert(x))


    # 1, 중복된 이름 검색 
    # 2.개수가 1인거 이상 뽑아내기 
    # 3.리스트만들기
    sd_cnt = Counter(sookso_diary.name)      # Counter 모듈로 name data count
    li =[]
    for name, cnt  in sd_cnt.items():            # 중복된 것만 뽑기 (cnt가 1이상이면 중복)
        if cnt > 1:
            li.append(name)

    # 4. 행가져와서 like집계, content, tag list append
    for i in li:           # 중복된 name 반복
        ex = sookso_diary.loc[sookso_diary.name == i]  # 반복된 name의 행을 가져온다
        ex.reset_index(drop=True,inplace=True)     # index reset

        content = ex.iloc[0,:].content
        date = ex.iloc[0,:].date
        like = ex.iloc[0,:].like
        place = ex.iloc[0,:].place
        tags = ex.iloc[0,:].tags
        imgUrl = ex.iloc[0,:].imgUrl
        name = ex.name[0]

        for i in range(len(ex)-1):
            like += ex.iloc[i+1,:].like
            tags += ex.iloc[i+1,:].tags



        # 5.차례대로 기존 행 지워내기
        indexNames = sookso_diary[sookso_diary['name'] == ex.name[0]].index
        sookso_diary.drop(indexNames, inplace=True) 
        sookso_diary = sookso_diary.append(pd.Series(data=[content,date,like,place,tags,imgUrl,name],index=sookso_diary.columns),ignore_index=True)

    sd_cnt_tmp = []
    for i in sookso_diary.name:
        sd_cnt_tmp.append(sd_cnt[i]-1)

    sookso_diary['overlap'] = sd_cnt_tmp

    #####################################################

    ### sookso.hada (완료)
    sookso_hada_name = sookso_hada.content.apply(lambda x : sookso_hada_extract(x))
    sookso_hada['name'] = sookso_hada_name
    sookso_hada = sookso_hada[~(sookso_hada['name'].isna())]

    # name data가 없는 index 지우기
    indexNames = sookso_hada[sookso_hada['name'] == ''].index
    sookso_hada.drop(indexNames, inplace=True) 


    # like integet 변환 후 합치기

    if sookso_hada.like.dtype == 'O':
        sookso_hada.like = sookso_hada.like.apply(lambda x : convert(x))


    # 1, 중복된 이름 검색 
    # 2.개수가 1인거 이상 뽑아내기 
    # 3.리스트만들기
    sh_cnt = Counter(sookso_hada.name)      # Counter 모듈로 name data count
    li =[]
    for name, cnt  in sh_cnt.items():            # 중복된 것만 뽑기 (cnt가 1이상이면 중복)
        if cnt > 1:
            li.append(name)        


    # 4. 행가져와서 like집계, content, tag list append
    for i in li:           # 중복된 name 반복
        ex = sookso_hada.loc[sookso_hada.name == i]  # 반복된 name의 행을 가져온다
        ex.reset_index(drop=True,inplace=True)     # index reset

        content = ex.iloc[0,:].content
        date = ex.iloc[0,:].date
        like = ex.iloc[0,:].like
        place = ex.iloc[0,:].place
        tags = ex.iloc[0,:].tags
        imgUrl = ex.iloc[0,:].imgUrl
        name = ex.name[0]

        for i in range(len(ex)-1):
            like += ex.iloc[i+1,:].like
            tags += ex.iloc[i+1,:].tags

        # 5.차례대로 기존 행 지워내기
        indexNames = sookso_hada[sookso_hada['name'] == ex.name[0]].index
        sookso_hada.drop(indexNames, inplace=True) 
        sookso_hada = sookso_hada.append(pd.Series(data=[content,date,like,place,tags,imgUrl,name],index=sookso_hada.columns),ignore_index=True)

    sh_cnt_tmp = []
    for i in sookso_hada.name:
        sh_cnt_tmp.append(sh_cnt[i]-1)

    sookso_hada['overlap'] = sh_cnt_tmp

    ######################################
    ##  데이터 프레임 합치기
    tot_dataset = daily_gamsung
    for df in [gamsung_bnb, gamsung_curation, hi_stay_tour, rest_behappyhere, sookso_diary, sookso_hada]:
        tot_dataset = tot_dataset.append(df)

    if tot_dataset.like.dtype == 'O':
        tot_dataset.like = tot_dataset.like.apply(lambda x : convert(x))



    dg_cnt = Counter(tot_dataset.name)      # Counter 모듈로 name data count
    li =[]
    for name, cnt  in dg_cnt.items():            # 중복된 것만 뽑기 (cnt가 1이상이면 중복)
        if cnt > 1:
            li.append(name)


    # 4. 행가져와서 like집계, content, tag list append
    for i in li:           # 중복된 name 반복
        ex = tot_dataset.loc[tot_dataset.name == i]  # 반복된 name의 행을 가져온다
        ex.reset_index(drop=True,inplace=True)     # index reset

        content = ex.iloc[0,:].content
        date = ex.iloc[0,:].date
        like = ex.iloc[0,:].like
        place = ex.iloc[0,:].place
        tags = ex.iloc[0,:].tags
        imgUrl = ex.iloc[0,:].imgUrl
        name = ex.name[0]
        overlap = ex.iloc[0,:].overlap

        for i in range(len(ex)-1):
            like += ex.iloc[i+1,:].like
            tags += ex.iloc[i+1,:].tags
            overlap += ex.iloc[i+1,:].overlap


        # 5.차례대로 기존 행 지워내기
        indexNames = tot_dataset[tot_dataset['name'] == ex.name[0]].index
        tot_dataset.drop(indexNames, inplace=True) 
        tot_dataset = tot_dataset.append(pd.Series(data=[content,date,like,place,tags,imgUrl,name,overlap],index=tot_dataset.columns),ignore_index=True)

    ####################################
    ## Naver 장소 API

    # naver application clientID,secret
    client_id = "6E6mBjvxbdEgVk6Prqgd"
    client_secret = "0ZjWnYiBjr"


    # quote가 인코딩을 변경(UTF8로 url에 넘겨주기 위해서)
    # 내가 입력한 값을 기준으로 parse
    title_et = []
    for name in tot_dataset.name:
        query = urllib.parse.quote(f'숙소+" "+{name}')
        idx = 0 # 요청 개수
        display = 10 # 100개 단위로 가져온다
        start = 1
        end = 100
        tmp=''

        url = "https://openapi.naver.com/v1/search/local?query=" + query \
            + "&display=" + str(display) \
            + "&start=" + str(start)


        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id",client_id)
        request.add_header("X-Naver-Client-Secret",client_secret)
        # request 요청에 대한 결과를 response에 저장
        response = urllib.request.urlopen(request)
        rescode = response.getcode()
        if(rescode==200):
            response_body = response.read()
            response_dict = json.loads(response_body.decode('utf-8'))
            items = response_dict['items']
            for item_index in range(0,len(items)):
                remove_tag = re.compile('<.*?>')
                title = re.sub(remove_tag, '', items[item_index]['title'])
                link = items[item_index]['link']
                caterory = items[item_index]['category']
                description = re.sub(remove_tag, '',items[item_index]['description'])
                telephone = items[item_index]['telephone']
                address = items[item_index]['address']
                roadAddress = items[item_index]['roadAddress']
                mapx = items[item_index]['mapx']
                mapy = items[item_index]['mapy']

                # 이름이 없는 경우
                if not title :
                    title_et.append('')
                else: #'전통숙소' '호스텔''리조트''리조트부속건물'
                    if caterory[:2] in ['숙박'] :  # 카테고리가 숙박인 경우
                        title = title.replace(' ','')
                        name = name.replace(' ','')
                        ratio = SequenceMatcher(None, title, name).ratio()
                        if ratio >= 0.7 : # title.replace(' ','') in name.replace(' ','')
                            tmp = address

            if tmp : title_et.append(tmp)
            else: title_et.append('')

        else:
            print("Error Code:" + rescode)

        time.sleep(0.1)


    tot_dataset.drop('place',axis=1,inplace=True)
    tot_dataset['place'] = title_et

    #####################################
    ### 코로나 단계 작업
    coronaStage = coronaStage.toPandas() 

    stage =[str() for i in range(len(tot_dataset))]
    local = [str() for i in range(len(tot_dataset))]
    for lo in coronaStage.Area.unique().tolist():
        for j in range(len(tot_dataset)):
            try :
                com = tot_dataset.iloc[j].place.split()[0]
                if list(lo)[0] in com and list(lo)[1] in com:
                    stage[j] = coronaStage[coronaStage.Area == lo].Stage.values[0]
                    local[j] = lo
            except:
                pass

    tot_dataset['stage'] = stage
    tot_dataset['local'] = local

    ############################
    ## 새로운 숙소 생성


    ch = []
    for i in cur.find():
        ch.append(i)
    change = pd.DataFrame(ch)

    a = tot_dataset.merge(change,how='left' ,on='name')
    li = a[a.tags_y.isnull() == True].index.tolist()
    new = tot_dataset.iloc[li]
    print(new)
#     new.drop('index',axis=1,inplace=True)
    new.to_parquet(f'/home/ubuntu/DE/new_{nowtime}.parquet')

def load_to_hdfs():
    import subprocess
    # move file to hdfs.
    nowtime = datetime.today().strftime("%Y-%m-%d")
    load_to_hadoop_script1 = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/daily_gamsung_' + nowtime + '.parquet', '/data/insta')
    load_to_hadoop_script2 = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/gamsung_bnb_' + nowtime + '.parquet', '/data/insta')
    load_to_hadoop_script3 = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/gamsung_curation_' + nowtime + '.parquet', '/data/insta')
    load_to_hadoop_script4 = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/hi_stay_tour_' + nowtime + '.parquet', '/data/insta')
    load_to_hadoop_script5 = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/rest_behappyhere_' + nowtime + '.parquet', '/data/insta')
    load_to_hadoop_script6 = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/sookso_diary_' + nowtime + '.parquet', '/data/insta')
    load_to_hadoop_script7 = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/sookso_hada_' + nowtime + '.parquet', '/data/insta')
    subprocess.call(load_to_hadoop_script1,shell=True)
    subprocess.call(load_to_hadoop_script2,shell=True)
    subprocess.call(load_to_hadoop_script3,shell=True)
    subprocess.call(load_to_hadoop_script4,shell=True)
    subprocess.call(load_to_hadoop_script5,shell=True)
    subprocess.call(load_to_hadoop_script6,shell=True)
    subprocess.call(load_to_hadoop_script7,shell=True)



def newdata_to_hdfs():
    import subprocess
    # move file to hdfs.
    nowtime = datetime.today().strftime("%Y-%m-%d")
    load_to_hadoop_script1 = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/new_' + nowtime + '.parquet', '/data/modeldata')
    subprocess.call(load_to_hadoop_script1,shell=True)
    




def token(docs):
    return docs

def insta_preporcessing(insta):
    # 데이터 로드
    insta_data = pd.read_csv(insta)

    # 1차 클린징(영어, 특수문자, 숫자제거)
    for i in range(len(insta_data.content)):
        insta_data.content[i] = sub_special(insta_data.content[i])

    # 토큰화
    for i in range(len(insta_data.content)):
        insta_data.content[i]= tokenize(insta_data.content[i])

    # 2차 클린징(불용어 처리)
        word_cleansing(insta_data)

    return insta_data


def naver_preprocessing(naver):
    #데이터 로드
    naver_data = pd.read_csv(naver)

    # 인덱스 제거
    naver_data.drop('Unnamed: 0', axis=1, inplace=True)

    # 토큰화
    for i in range(len(naver_data)):
        naver_data.content[i] = sub_special_token(naver_data.content[i])

    #클린징
    word_cleansing(naver_data)

    return naver_data

def make_stopwords(input_text):
    with open (input_text, 'r', encoding= 'utf-8') as f:
        words = f.readline().split()
    stop_words = '의 가 이 은 들 는 좀 잘 속초 걍 과 했 거 해서 숙박 경기도 떠나다 느끼다 비애 댓글 권 만원 리조트 카라 추첨 즐기다 다녀오다 출처 여행 제공 감사 게 찍 느낌 많이 듯 뷰 박 링크 인스타그램 할인 성수기 홈페이지 주말 블로그 인원 추가 ㅎㅎ 너무 게스트 넘 하우스 드리다 이용 위치 쓰다 진짜 넘 찍 거 먹 ㅠㅠ ㅎㅎㅎ ㅠ 물 였 ㅠㅠㅠ ㅠㅠ ㅠ ㅋㅋㅋ ㅋㅋ ㅋ ㅎㅎㅎ ㅎㅎ ㅎ 화장실 도 뭐 오픈 최대 준비 룸 빵 거 많이 방법 달리 스럽다 특별자치도 를 에어비 으로 자 에 와 한 하다 아 휴 아이구 포스팅 아이쿠 아이고 어 나 우리 저희 따라 의해 을 를 에 의 가 으로 로 에게 뿐이다 의거하여 근거하여 입각하여 기준으로 예하면 예를 들면 예를 들자면 저 소인 소생 저희 지말고 하지마 하지마라 다른 물론 또한 그리고 비길수 없다 해서는 안된다 뿐만 아니라 만이 아니다 만은 아니다 막론하고 관계없이 그치지 않다 그러나 그런데 하지만 든간에 논하지 않다 따지지 않다 설사 비록 더라도 아니면 만 못하다 하는 편이 낫다 불문하고 향하여 향해서 향하다 쪽으로 틈타 이용하여 타다 오르다 제외하고 이 외에 이 밖에 하여야 비로소 한다면 몰라도 외에도 이곳 여기 부터 기점으로 따라서 할 생각이다 하려고하다 이리하여 그리하여 그렇게 함으로써 하지만 일때 할때 앞에서 중에서 보는데서 으로써 로써 까지 해야한다 일것이다 반드시 할줄알다 할수있다 할수있어 임에 틀림없다 한다면 등 등등 제 겨우 단지 다만 할뿐 딩동 댕그 대해서 대하여 대하면 훨씬 얼마나 얼마만큼 얼마큼 남짓 이제 분 도움 여 ㅁ ㅎ ㄶ 얼마간 둥 오랜만 약간 체크 체크아웃 가격 정보 비 수기 평일 기준 약 전국  예약 되어다 스마트 빔 블루투스 스피커 이 외 드라이어 다리미 구비 되어다 있다 주방 음식 조리 가능하다 환기 제한 전남 제부도 있다 냄새 나 요리 삼가다 경주시 부탁드리다 후 번길 문의 다 가격 수기 비성수기 주소  좀 조금 다수 몇 얼마 지만 하물며 또한 그러나 그렇지만 하지만 이외에도 대해 말하자면 뿐이다 다음에 반대로 반대로 말하자면 이와 반대로 바꾸어서 말하면 바꾸어서 한다면 만약 그렇지않으면 까악 툭 딱 삐걱거리다 보드득 비걱거리다 꽈당 응당 해야한다 에 가서 각 각각 여러분 각종 각자 제각기 하도록하다 와 과 그러므로 그래서 고로 한 까닭에 하기 때문에 거니와 이지만 대하여 관하여 관한 과연 실로 아니나다를가 생각한대로 진짜로 한적이있다 하곤하였다 하 하하 허허 아하 거바 와 오 왜 어째서 무엇때문에 어찌 하겠는가 무슨 어디 어느곳 더군다나 하물며 더욱이는 어느때 언제 야 이봐 어이 여보시오 흐흐 흥 휴 헉헉 헐떡헐떡 영차 여차 어기여차 끙끙 아야 앗 아야 콸콸 졸졸 좍좍 뚝뚝 주룩주룩 솨 우르르 그래도 또 그리고 바꾸어말하면 바꾸어말하자면 혹은 혹시 답다 및 그에 따르는 때가 되어 즉 지든지 설령 가령 하더라도 할지라도 일지라도 지든지 몇 거의 하마터면 인젠 이젠 된바에야 된이상 만큼 어찌됏든 그위에 게다가 점에서 보아 비추어 보아 고려하면 하게될것이다 일것이다 비교적 좀 보다더 비하면 시키다 하게하다 할만하다 의해서 연이서 이어서 잇따라 포항 우도 양양 전주시 통영 제천 여수시 순천 고성 합천 한림읍 전주 경북 구좌읍 돌산읍 태안 하동 포항시 제주시 밀양 양평 울산 무무 뒤따라 뒤이어 결국 의지하여 기대여 통하여 자마자 더욱더 불구하고 얼마든지 마음대로 주저하지 않고 곧 즉시 바로 당장 하자마자 밖에 안된다 하면된다 그래 그렇지 요컨대 다시 말하자면 바꿔 춘천 층 남해 스테이 서울 여수 거제 추천 경주 곳 객실 이다 강원도 강원 홍천 부산 영도 호 객실 교동 풀빌라 빌라 풀 스튜디오 펜션 말하면 즉 구체적으로 말하자면 시작하여 시초에 이상 허 헉 허걱 바와같이 해도좋다 해도된다 게다가 더구나 하물며 와르르 팍 퍽 펄렁 동안 이래 하고있었다 이었다 에서 로부터 까지 예하면 했어요 해요 함께 같이 더불어 마저 마저도 양자 모두 습니다 가까스로 하려고하다 즈음하여 다른 다른 방면으로 해봐요 습니까 했어요 말할것도 없고 무릎쓰고 개의치않고 하는것만 못하다 하는것이 낫다 매 매번 들 모 어느것 어느 로써 갖고말하자면 어디 어느쪽 어느것 어느해 어느 년도 라 해도 언젠가 어떤것 어느것 저기 저쪽 저것 그때 그럼 그러면 요만한걸 그래 그때 저것만큼 그저 이르기까지 할 줄 안다 할 힘이 있다 너 너희 당신 어찌 설마 차라리 할지언정 할지라도 할망정 할지언정 구토하다 게우다 토하다 메쓰겁다 옆사람 퉤 쳇 의거하여 근거하여 의해 따라 힘입어 그 다음 버금 두번째로 기타 첫번째로 나머지는 그중에서 견지에서 형식으로 쓰여 입장에서 위해서 단지 의해되다 하도록시키다 뿐만아니라 반대로 전후 전자 앞의것 잠시 잠깐 하면서 그렇지만 다음에 그러한즉 그런즉 남들 아무거나 어찌하든지 같다 비슷하다 예컨대 이럴정도로 어떻게 만약 만일 위에서 서술한바와같이 인 듯하다 하지 않는다면 만약에 무엇 무슨 어느 어떤 아래윗 조차 한데 그럼에도 불구하고 여전히 심지어 까지도 조차도 하지 않도록 않기 위하여 때 시각 무렵 시간 동안 어때 어떠한 하여금 네 예 우선 누구 누가 알겠는가 아무도 줄은모른다 줄은 몰랏다 하는 김에 겸사겸사 하는바 그런 까닭에 한 이유는 그러니 그러니까 때문에 그 너희 그들 너희들 타인 것 것들 너 위하여 공동으로 동시에 하기 위하여 어찌하여 무엇때문에 붕붕 윙윙 나 우리 엉엉 휘익 윙윙 오호 아하 어쨋든 만 못하다 하기보다는 차라리 하는 편이 낫다 흐흐 놀라다 상대적으로 말하자면 마치 아니라면 쉿 그렇지 않으면 그렇지 않다면 안 그러면 아니었다면 하든지 아니면 이라면 좋아 알았어 하는것도 그만이다 어쩔수 없다 하나 일 일반적으로 일단 한켠으로는 오자마자 이렇게되면 이와같다면 전부 한마디 한항목 근거로 하기에 아울러 하지 않도록 않기 위해서 이르기까지 이 되다 로 인하여 까닭으로 이유만으로 이로 인하여 그래서 이 때문에 그러므로 그런 까닭에 알 수 있다 결론을 낼 수 있다 으로 인하여 있다 어떤것 관계가 있다 관련이 있다 연관되다 어떤것들 에 대해 이리하여 그리하여 여부 하기보다는 하느니 하면 할수록 운운 이러이러하다 하구나 하도다 다시말하면 다음으로 에 있다 에 달려 있다 우리 우리들 오히려 하기는한데 어떻게 어떻해 어찌됏어 어때 어째서 본대로 자 이 이쪽 여기 이것 이번 이렇게말하자면 이런 이러한 이와 같은 요만큼 요만한 것 얼마 안 되는 것 이만큼 이 정도의 이렇게 많은 것 이와 같다 이때 이렇구나 것과 같이 끼익 삐걱 따위 와 같은 사람들 부류의 사람들 왜냐하면 중의하나 오직 오로지 에 한하다 하기만 하면 도착하다 까지 미치다 도달하다 정도에 이르다 할 지경이다 결과에 이르다 관해서는 여러분 하고 있다 한 후 혼자 자기 자기집 자신 우에 종합한것과같이 총적으로 보면 총적으로 말하면 총적으로 대로 하다 으로서 참 그만이다 할 따름이다 쿵 탕탕 쾅쾅 둥둥 봐 봐라 아이야 아니 와아 응 아이 참나 년 월 일 령 영 일 이 삼 사 오 육 륙 칠 팔 구 이천육 이천칠 이천팔 이천구 하나 둘 셋 넷 다섯 여섯 일곱 여덟 아홉 령 영 이 있 하 것 들 그 되 수 이 보 않 없 나 사람 주 아니 등 같 우리 때 년 가 한 지 대하 오 말 일 그렇 위하 때문 그것 두 말하 알 그러나 받 못하 일 그런 또 문제 더 사회 많 그리고 좋 크 따르 중 나오 가지 씨 시키 만들 지금 생각하 그러 속 하나 집 살 모르 적 월 데 자신 안 어떤 내 내 경우 감성 숙소 호텔 제주 제주도 강원도 강릉 속초 속초시 에어비앤비 명 생각 시간 그녀 수 약 다시 이런 앞 보이 번 나 다른 어떻 여자 개 전 들 사실 이렇 점 싶 말 정도 좀 원 잘 통하 놓 사실 이렇 점 싶 말 정도 좀 원 잘 통하 ㅆ 재 채 독 롭 도란도 팅 감성비앤비 공간 참여 이벤트 당첨자 기간 부산 #부산숙소 #부산에어비앤비 #부산여행 경기 경주시 있는 한 #경주숙소 강원도 제주 제주시 #제주숙소 #제주감성숙소 #제주숙소추천 남해군 서면 춘천시 남해 광안리 편 반 안면도 산방산 님 사장 정말 공주 안동 충남 전라북도 충청남도 전북 실시간 퇴 활용 상품권 이메일 경상남도 날 꼭 찾다 직접 날 그냥 맘 근데 스타 욕 편 째 첫 맛 채 황리단 황리단길 미리 열다 거제도 공간 광안리 광안대교 스다 해운대 헤이 윤슬 울릉도 손 옆 홍보 입실시간 에디터경기도 캠핑 청주 가평 청도 포천 핑 강화도 근교 인천' #불용어 리스트 형성
    stop_words = stop_words.split(' ')
    stop_location = pd.read_csv('/home/ubuntu/DS/location_words.csv')
    stop_location.columns = ['index', 'location']
    stop_location = stop_location.location.tolist()

    stop = stop_words + stop_location + words
    stop = pd.Series(stop)
    stopwords = stop.unique().tolist()

    return stopwords

# 토크나이징 함수 정의 


# tokenize 함수를 정의합니다. 한국어 문장을 입력하면 형태소 단위로 분리하고, 
# 불용어 및 특수 문자 등을 제거한 뒤, list로 반환합니다.

def make_userdic(input_text):
    with open (input_text, 'r', encoding= 'utf-8') as f:
        words = f.readline().split()
    kiwi.add_user_word(words) # 사용자사전


# tokenize 함수를 정의합니다. 한국어 문장을 입력하면 형태소 단위로 분리하고, 
# 불용어 및 특수 문자 등을 제거한 뒤, list로 반환합니다.

def tokenize(sent):
    from kiwipiepy import Kiwi
    stoptext = '/home/ubuntu/DS/stopwords_korean.txt'
    stopwords = make_stopwords(stoptext)

    user_word= '호캉스  호텔  힐링  독채숙소  독채  한옥  자연  오션뷰 외국 캠핑 바다 파티 럽스타 어메니티 빔프로젝트 빔 프로젝트 글램핑 풀빌라 노천탕 프라이빗 인피니티풀 루프탑 히노끼탕 히노끼 온수풀 수영장 숲속 숲 나무'
    # naver = './total_blog_dataset.csv'
    kiwi = Kiwi()
    kiwi.add_user_word(user_word, 'NNP', 10)
    kiwi.prepare()
    time.sleep(5)

    res, score = kiwi.analyze(sent)[0] # 첫번째 결과를 사용
    return [word + ('다' if tag.startswith('V') else '') # 동사에는 '다'를 붙여줌
            for word, tag, _, _ in res
            if not tag.startswith('E') and not tag.startswith('J') and not tag.startswith('S') and not tag.startswith('W') and word not in stopwords] # 조사, 어미, 특수기호 및 stopwords에 포함된 단어는 제거


# def tokenize_nouns(sent):
#     res, score = kiwi.analyze(sent)[0] # 첫번째 결과를 사용
#     return [word + ('다' if tag.startswith('V') else '') # 동사에는 '다'를 붙여줌
#             for word, tag, _, _ in res
#             if tag.startswith('N')] # 조사, 어미, 특수기호 및 stopwords에 포함된 단어는 제거


"""인스타 데이터 전처리"""

#"""한글빼고 전부 제거"""
def sub_special(s):
    rs = re.sub(r'[^가-힣]',' ',s)
    rr = re.sub(' +', ' ', rs)
    return rr

#"""한글빼고 전부 제거"""
def sub_special_token(s):
    rs = re.sub(r'[^가-힣]',' ',s).strip().split()
    return rs

#불용어 제거
def word_cleansing(data): 
    stoptext = '/home/ubuntu/DS/stopwords_korean.txt'
    stopwords = make_stopwords(stoptext)
    for i in range(len(data)): 
        result = []
        for w in data.content[i]:
            if w not in stopwords:
                result.append(w)
    
        data.content[i] = result
    return data
        
def make_total_list(naver_data, insta_data):
    n_data = pd.DataFrame(naver_data.copy()) # 네이버 블로그 데이터 로드(콘텐츠)
    i_data = pd.DataFrame(insta_data[['index', 'content']].copy()) # 인스타 데이터 로드(인덱스, 콘텐츠)

    i_data.columns = ['item_idx', 'content'] # merge를 위한 식별자 컬럼('item_idx') 생성

    all_items = pd.concat([i_data, n_data]) # 두 데이터 행 병합

    all_items.reset_index(drop=True, inplace=True) #인덱스 리셋

    #빈 샘플 확인 및 제거
    drop = [index for index in range(len(all_items)) if len(all_items.content[index]) < 1] # drop에 empty 샘플의 인덱스를 저장


    while len(drop) > 0: 
        # 빈 샘플 제거
        for i in drop:
            all_items.drop(all_items.index[i], inplace=True)
        # 인덱스 리셋
        all_items.reset_index(drop=True, inplace=True)

        drop = [index for index in range(len(all_items)) if len(all_items.content[index]) < 1] # 재확인 

     # 인덱스 리셋
    all_items.reset_index(drop=True, inplace=True) #최종 리셋

    return all_items



def create_model(data, clusters_n):
    word_vectors = data['wv'].to_list() 
    num_clusters = clusters_n

    # K means 를 정의하고 학습시킨다.
    kmeans_clustering = KMeans(n_clusters=num_clusters, init='k-means++', n_init=25, max_iter = 600, random_state=0)
    idx = kmeans_clustering.fit_predict( word_vectors )
    data['category'] = idx
    
def get_sentence_mean_vector(morphs):
    # from gensim.models import Word2Vec
    # embedding_model = Word2Vec(new_items.content, vector_size=100, window = 2, min_count=3, workers=4, epochs=200, sg=1, seed=0)
    vector = []
    for i in morphs:
        try:
            vector.append(embedding_model.wv[i])
        except KeyError as e:
            pass
    try:
        return sum(vector)/len(vector)
    except IndexError as e:
        pass
    
def token_dummy(docs):
    return docs

def embedding(data):
    embedding_model = Word2Vec(data.content, vector_size=100, window = 2, min_count=3, workers=4, epochs=100, sg=1)
    return embedding_model

def keyword_extractor():
    keyword_extractor = KeywordSummarizer(tokenize = token_dummy,
    min_count=5,
    window=8,                     # cooccurrence within a sentence
    min_cooccurrence=3,
    vocab_to_idx=None,             # you can specify vocabulary to build word graph
    df=0.85,                       # PageRank damping factor
    max_iter=30,                   # PageRank maximum iteration
    verbose=False)
    
    return keyword_extractor

def cos_sim(A, B):
       return dot(A, B)/(norm(A)*norm(B))

    
def create_matrix(data):
    wv_matrix = np.asarray(data.wv)
    rows = []
    matrix = []
    for i in range(len(data.content)):
        for x in range(len(data.content)):
            cos_sim = dot(wv_matrix[i], wv_matrix[x])/(norm(wv_matrix[i])*norm(wv_matrix[x]))
            rows.append(cos_sim)
        matrix.append(rows)
        rows=[]
    return matrix

def create_model_spherical(data, n_clusters):
    x = data.wv.to_list()
    x = np.asarray(x)
    csr = csr_matrix(x)
    spherical_kmeans = SphericalKMeans(
    n_clusters=n_clusters,
    max_iter=15,
    verbose=1,
    init='similar_cut')
    time.sleep(1)
    index = spherical_kmeans.fit_predict(csr)
    data['category'] = index
    
def token(docs):
    return docs

def data_embedding(wv_data):
    data_embedding = []
    for i in wv_data:
        data_embedding.append(i)
    data_vector = np.array(data_embedding)
    
    return data_vector

def create_data_matrix(data_vector, item_vector, basedata):
    item_vector = np.asarray(item_vector)
    row = []
    data_matrix = []
    for i in range(len(data_vector)):
        for x in range(len(data.content)):
            cos_sim = dot(data_vector[i], item_vector[x])/(norm(data_vector[i])*norm(item_vector[x]))
            rows.append(cos_sim)
        matrix.append(rows)
        rows=[]
   
    return user_matrix

def data_recommand(data_matrix, basedata):
    matrix = np.array(data_matrix).reshape(-1)
    scores = np.argsort(-matrix)[:100]
    result = basedata.loc[scores, :]
    
    return result

def category_infer(result): #코사인 유사도 기반 추천 목록을 인풋값으로 활용함
    count = dict(result.category.value_counts())
    for k,v in count.items():
        a = []
        a.append(k)
        new_category = a[0]
        
    return new_category

def create_new_matrix(newdata, basedata):
    item_vector = np.asarray(basedata.wv)
    data_vector = np.array(newdata.wv.to_list())
    rows = []
    matrix = []
    for i in range(len(data_vector)):
        for x in range(len(basedata.content)):
            cos_sim = dot(data_vector[i], item_vector[x])/(norm(data_vector[i])*norm(item_vector[x]))
            rows.append(cos_sim)
        matrix.append(rows)
        rows=[]
    return matrix

def match_category(matrix, newdata, basedata):
    matrix = np.array(matrix)
    scores = np.argsort(-matrix)
    total = []
    for x in range(len(newdata)):
        index = []
        for i in range(100):
            temp = np.where(scores[x] == i)[0][0]
            index.append(temp)
            result = basedata.loc[index, :]
            category = category_infer(result)
        total.append(category)
    newdata['category'] = total
    
    return newdata


#######################################################################################################
# 데이터 로드


def data_load():
    from kiwipiepy import Kiwi
    import pandas as pd
    import numpy as np
    import re
    import time
    from kiwipiepy import Kiwi

    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    from pyspark.sql.functions import array_contains, udf
    from datetime import datetime

    from gensim.models import Word2Vec
    from numpy import dot
    from numpy.linalg import norm
    import numpy as np
    from scipy.sparse import csr_matrix

    from gensim.models import Word2Vec


    nowtime = datetime.today().strftime("%Y-%m-%d")
    spark = SparkSession        .builder        .appName('aaa')        .getOrCreate()

    base_data = spark.read.parquet(f"hdfs://localhost:9000/data/modeldata/tot_dataset_2021-10-04.parquet")
    base_data = base_data.toPandas()


    new_data = spark.read.parquet(f"hdfs://localhost:9000/data/modeldata/new_{nowtime}.parquet")
    new_data = new_data.toPandas()

    new_data.reset_index(drop = True, inplace=True)
    original = new_data.content.copy()

    ####################################################################################################

    #형태소 분석기 / 불용어리스트 생성
    stoptext = '/home/ubuntu/DS/stopwords_korean.txt'
    stopwords = make_stopwords(stoptext) #불용어 리스트 생성
    # user_word= '호캉스  호텔  힐링  독채숙소  독채  한옥  자연  오션뷰 외국 캠핑 바다 파티 럽스타 어메니티 빔프로젝트 빔 프로젝트 글램핑 풀빌라 노천탕 프라이빗 인피니티풀 루프탑 히노끼탕 히노끼 온수풀 수영장 숲속 숲 나무'
    # naver = './total_blog_dataset.csv'
    # kiwi = Kiwi()
    # kiwi.add_user_word(user_word, 'NNP', 10)
    # kiwi.prepare()
    # time.sleep(10)
    # 1차 클린징(영어, 특수문자, 숫자제거)
    for i in range(len(new_data.content)):
        new_data.content[i] = sub_special(new_data.content[i])
    time.sleep(10)
    # 토큰화
    for i in range(len(new_data.content)):
        new_data.content[i]= tokenize(new_data.content[i])
    time.sleep(10)
    # 2차 클린징(불용어 처리)
    word_cleansing(new_data)

    time.sleep(1)

    new_items = new_data.copy()
    time.sleep(5)
    global embedding_model
    embedding_model = Word2Vec(new_items.content, vector_size=100, window = 2, min_count=3, workers=4, epochs=200, sg=1, seed=0)
    time.sleep(5)
    new_items['wv'] = new_items['content'].map(get_sentence_mean_vector)

    time.sleep(1)

    # data_vector = data_embedding(new_items.wv)
    # data_matrix = create_data_matrix(data_vector, new_items.wv, new_items)
    # result = data_recommand(data_matrix, new_items)

    # new_category = category_infer(result)

    matrix = create_new_matrix(new_items, base_data)
    new_data = match_category(matrix, new_items, base_data)


    # time.sleep(1)
    new_data.drop('__index_level_0__', axis = 1, inplace = True)
    new_data['original_content'] = original
    new_data['category_label'] = ' '

    new_data.to_parquet(f'/home/ubuntu/DE/new_data_{nowtime}.parquet')


def newdata_to_hdfs_fin():
    import subprocess
    # move file to hdfs.
    nowtime = datetime.today().strftime("%Y-%m-%d")
    load_to_hadoop_script = "hdfs dfs -moveFromLocal {0} {1}".format('/home/ubuntu/DE/new_data_' + nowtime + '.parquet', '/data/modeldata')
    subprocess.call(load_to_hadoop_script,shell=True)


# hdfs -> mongoDB 이동 newdata_to_mongo
def newdata_to_mongo():
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

    newdata = spark.read.parquet(f"hdfs://localhost:9000/data/modeldata/new_data_{nowtime}.parquet")
  
    for i in newdata.collect():
        db.cluster.insert_one(i.asDict())

    



####################
# Dag Task Setting
####################

csv_to_parquet = PythonOperator(
        task_id = 'csvToparquet',
        python_callable = insta_call,       
        #provide_context=True,
        dag = dag
        )
        

parquet_load_to_HDFS = PythonOperator(
    task_id = 'parquet_move',
    python_callable = load_to_hdfs,
    dag = dag
    )


new_data_create = PythonOperator(
    task_id = 'create_newdata',
    python_callable = insta_spark_processing,       
    #provide_context=True,
    dag = dag
    )

# hdfs 적재
newdata_hdfs = PythonOperator(
    task_id = 'newdata_parquet_move',
    python_callable = newdata_to_hdfs,       
    #provide_context=True,
    dag = dag
    )

# new data 데이터 로드
newdata_load = PythonOperator(
    task_id = 'newdata_load',
    python_callable = data_load,       
    #provide_context=True,
    dag = dag
    )

# newdata_to_hdfs
newdata_hdfs_load = PythonOperator(
    task_id = 'newdata_hdfs_load',
    python_callable = newdata_to_hdfs_fin,       
    #provide_context=True,
    dag = dag
    )


# hdfs -> mongoDB 이동
newdata_mongo_move = PythonOperator(
    task_id = 'newdata_mongo_move',
    python_callable = newdata_to_mongo,       
    #provide_context=True,
    dag = dag
    )

#Assign the order of the tasks in our DAG
csv_to_parquet >> parquet_load_to_HDFS >> new_data_create >> newdata_hdfs >> newdata_load >> newdata_hdfs_load >> newdata_mongo_move