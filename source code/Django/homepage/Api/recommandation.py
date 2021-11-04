import pandas as pd
import numpy as np

from homepage.MongoDbManager import  MongoDbManager_insta
import pymongo

def get_matrix( _query={}):
    client = pymongo.MongoClient( host='localhost',
                                  port=27017)
    database = client['ojo_db']['matrix']
    return database.find(_query)


def extract_name(name):
    client = pymongo.MongoClient( host='localhost',
                                  port=27017)
    database = client['ojo_db']['insta']
    return database.find({'name' : name },{'_id':0, 'doc_index' : 1})

def extract_category(name):
    client = pymongo.MongoClient( host='localhost',
                                  port=27017)
    database = client['ojo_db']['insta']
    return database.find({'name' : name },{'_id':0})

def find_name(insta_indices):
    client = pymongo.MongoClient( host='localhost',
                                  port=27017)
    database = client['ojo_db']['insta']
    return database.find({'doc_index' : {'$in': insta_indices }},{'_id':0, 'name':1})

def retrive_matrix(): #매트릭스 재구성
    matrix = [] # 빈리스트 생성
    # result = list(MongoDbManager_matrix.get_data_from_collection({'_id':0})) #리스트 형태로 몽고db 데이터 호출
    result = list(get_matrix()) #리스트 형태로 몽고db 데이터 호출
    for i in range(len(result)):
        a = list(result[i].values()) #순차적으로 values 추출
        matrix.append(a[1]) # values에서 vector 값만 뽑아 빈리스트에 2차원 형태로 붙이기
    matrix = np.array(matrix) # array 형태로 변환
    
    return matrix

# def read_pandas(query = {}, no_id = True):
    
#     client = pymongo.MongoClient( host='localhost',
#                                   port=27017)
#     database = client['ojo_db']['cluster']
    
#     cursor = database.find(query)

#     # Expand the cursor and construct the DataFrame
#     df =  pd.DataFrame(list(cursor))

#     # Delete the _id
#     if no_id:
#         del df['_id']

#     return df


def insta_REC(name):
    context = {}
    cosine_sim= retrive_matrix()
    ##인덱스 테이블 만들기##
    # indices = pd.Series(insta_data.index, index=insta_data.name).drop_duplicates()
    
    #입력한 숙소로부터 인덱스 가져오기
    # idx = indices[name] # doc_idx
    idx = list(extract_name(name)) # doc_idx
    idx = int(idx[0]['doc_index'])

    #입력한 숙소로부터 카테고리 가져오기



    # 모든 숙소에 대해서 해당 숙소와의 유사도를 구하기
    sim_scores = list(enumerate(cosine_sim[idx]))

    # 유사도에 따라 숙소들을 정렬
    sim_scores = sorted(sim_scores, key=lambda x:x[1], reverse = True)

    # 가장 유사한 10개의 숙소를 받아옴
    sim_scores = sim_scores[1:11] 

    # 가장 유사한 10개 숙소의 인덱스 받아옴
    insta_indices = [i[0] for i in sim_scores]
    
    #기존에 읽어들인 데이터에서 해당 인덱스의 값들을 가져온다. 그리고 스코어 열을 추가하여 코사인 유사도도 확인할 수 있게 한다.
    # result_data = insta_data.iloc[insta_indices].copy()
    # result_data['score'] = [i[1] for i in sim_scores]
    result_data = find_name(insta_indices)
    
    # for i in range(len(result_data)):
    for i in range(0,10):
        context[f'b{i}'] = result_data[i]['name']
    # 가장 유사한 10개의 숙소의 제목을 리턴
    return context