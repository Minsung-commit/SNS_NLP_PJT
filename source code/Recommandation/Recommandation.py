#!/usr/bin/env python
# coding: utf-8

# In[11]:


# 필수 모듈 호출
import pandas as pd
import numpy as np
import re
# Word2Vec embedding
from gensim.models import Word2Vec
from numpy import dot
from numpy.linalg import norm
import numpy as np

from numpy import dot
from numpy.linalg import norm
import numpy as np

# 데이터 로드
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import array_contains, udf
from datetime import datetime

spark = SparkSession        .builder        .appName('bbb')        .getOrCreate()
nowtime = datetime.today().strftime("%Y-%m-%d")
insta_data = spark.read.parquet(f"hdfs://localhost:9000/data/modeldata/merged_data_{nowtime}")
insta_data = insta_data.toPandas()


def get_sentence_mean_vector(morphs):
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

def cos_sim(A, B):
       return dot(A, B)/(norm(A)*norm(B))

    
def create_matrix(wv_matrix):
    rows = []
    matrix = []
    for i in range(len(insta_data.content_x)):
        for x in range(len(insta_data.content_x)):
            cos_sim = dot(wv_matrix[i], wv_matrix[x])/(norm(wv_matrix[i])*norm(wv_matrix[x]))
            rows.append(cos_sim)
        matrix.append(rows)
        rows=[]
    return matrix



def insta_REC(name):
    cosine_sim=matrix
    ##인덱스 테이블 만들기##
    indices = pd.Series(insta_data.index, index=insta_data.name).drop_duplicates()
    
    #입력한 숙소로부터 인덱스 가져오기
    idx = indices[name]

    # 모든 숙소에 대해서 해당 숙소와의 유사도를 구하기
    sim_scores = list(enumerate(cosine_sim[idx]))

    # 유사도에 따라 숙소들을 정렬
    sim_scores = sorted(sim_scores, key=lambda x:x[1], reverse = True)

    # 가장 유사한 10개의 숙소를 받아옴
    sim_scores = sim_scores[1:11]

    # 가장 유사한 10개 숙소의 인덱스 받아옴
    insta_indices = [i[0] for i in sim_scores]
    
    #기존에 읽어들인 데이터에서 해당 인덱스의 값들을 가져온다. 그리고 스코어 열을 추가하여 코사인 유사도도 확인할 수 있게 한다.
    result_data = insta_data.iloc[insta_indices].copy()
    result_data['score'] = [i[1] for i in sim_scores]
    
    # 읽어들인 데이터에서 콘텐츠 부분만 제거, 제목과 스코어만 보이게 함
    # del result_data['content']
    del result_data['wv']
    # del result_data['token_nolist']

    # 가장 유사한 10개의 숙소의 제목을 리턴
    return result_data.name,  result_data['score']


embedding_model = Word2Vec(insta_data.content_x, vector_size=100, window = 2, min_count=3, workers=4, epochs=100, sg=1, seed=0)
insta_data['wv'] = insta_data['content_x'].map(get_sentence_mean_vector)
wv_matrix = np.asarray(insta_data.wv)
matrix = create_matrix(wv_matrix)

insta_REC('스튜디오노이')


# # Word2Vec embedding

# In[8]:


embedding_model = Word2Vec(insta_data.content_x, vector_size=100, window = 2, min_count=3, workers=4, epochs=100, sg=1)


# ### 문장 벡터

# In[9]:


insta_data['wv'] = insta_data['content_x'].map(get_sentence_mean_vector)


# ## 코사인 유사도 행렬 생성

# In[10]:


wv_matrix = np.asarray(insta_data.wv)

matrix = create_matrix(wv_matrix)


# In[12]:


np.shape(matrix)


# In[24]:


##인덱스 테이블 만들기##
indices = pd.Series(insta_data.index, index=insta_data.name).drop_duplicates()
print(indices)


# ## 유사 아이템 검색

# In[13]:


def insta_REC(name, cosine_sim=matrix):
    
    ##인덱스 테이블 만들기##
    indices = pd.Series(insta_data.index, index=insta_data.name).drop_duplicates()
    
    #입력한 숙소로부터 인덱스 가져오기
    idx = indices[name]

    # 모든 숙소에 대해서 해당 숙소와의 유사도를 구하기
    sim_scores = list(enumerate(cosine_sim[idx]))

    # 유사도에 따라 숙소들을 정렬
    sim_scores = sorted(sim_scores, key=lambda x:x[1], reverse = True)

    # 가장 유사한 10개의 숙소를 받아옴
    sim_scores = sim_scores[1:11]

    # 가장 유사한 10개 숙소의 인덱스 받아옴
    insta_indices = [i[0] for i in sim_scores]
    
    #기존에 읽어들인 데이터에서 해당 인덱스의 값들을 가져온다. 그리고 스코어 열을 추가하여 코사인 유사도도 확인할 수 있게 한다.
    result_data = insta_data.iloc[insta_indices].copy()
    result_data['score'] = [i[1] for i in sim_scores]
    
    # 읽어들인 데이터에서 콘텐츠 부분만 제거, 제목과 스코어만 보이게 함
    # del result_data['content']
    del result_data['wv']
    # del result_data['token_nolist']

    # 가장 유사한 10개의 숙소의 제목을 리턴
    return result_data.name


# In[15]:


insta_REC('스튜디오노이')


# In[16]:


from pymongo import MongoClient
from datetime import datetime

client = MongoClient('localhost',27017) # mongodb 27017 port
db = client.ojo_db


# In[74]:


matrix = []
result = list(db.matrix.find())


# In[75]:


for i in range(len(result)):
    a = list(result[i].values())
    matrix.append(a[1])


# In[77]:


matrix = np.array(matrix)


# In[78]:


matrix


# In[83]:


def retrive_matrix(): #매트릭스 재구성
    matrix = [] # 빈리스트 생성
    result = list(db.matrix.find()) #리스트 형태로 몽고db 데이터 호출
    for i in range(len(result)):
        a = list(result[i].values()) #순차적으로 values 추출
        matrix.append(a[1]) # values에서 vector 값만 뽑아 빈리스트에 2차원 형태로 붙이기
    matrix = np.array(matrix) # array 형태로 변환
    
    return matrix


# In[ ]:




