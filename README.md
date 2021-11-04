# SNS_NLP_PJT
Natural Language Processing Project based on SNS text data  

# 프로젝트명 : 코로나시대, 안전한 여행을 위한 SNS기반 감성숙소 추천서비스
## 프로젝트 개요
  - 코로나 시대 속 안전하고 다양한 여행을 위한 숙소 정보 제공 및 추천 서비스를 구현함.
### 활용 데이터 
  - 인스타그램 감성숙소 정보공유 게시글 & 네이버 블로그 리뷰 데이터
### 활용 기술
  - 웹 크롤링, Kiwi, Word2Vec, K-means, Content based Filltering, TextRank, HDFS, MongoDB, Airflow, Django 
### 분석 방식
  1. 크롤링을 통한 데이터 수집
  2. 1차 전처리(특수문자 제거, 문자열 합치기 등)
  3. Kiwi 형태소 분석기를 통한 토큰화
  4. 2차 전처리(불용어 처리, 공백 제거 등)
  5. EDA / W2v을 활용한 Vectorization 
  6. Spherical K-means Clustering
  7. TextRank를 통한 군집 해석 및 라벨링 
  8. 코사인 유사도를 적용한 아이템 기반 필터링 알고리즘 구축
  9. 군집 분석을 기반으로 한 추론 알고리즘 구축

#### 문제 해결 과정
  1. 최초 접근은 Tomotopy의 LDA를 활용하여 클러스터링 및 토픽 모델링을 시도
  2. 결과 해석이 어렵고, 내부 알고리즘이 블랙박스 형태로 유지되어 정확한 이해 및 설명이 어려움
  3. 기존 방식을 파기하고, K-means를 활용한 클러스터링으로 선회하였으나, 모델 성능이 높지 않음
  4. 원인 분석 결과, 기존 K-means은 유클리디안 거리법을 적용하지만 이 기법은 고차원 벡터를 다루는 텍스트 분석(W2V을 통해 200차원으로 설정)에는 적절하지 않음을 알게됨.
  5. 이 문제를 해결하기 위해 K-means의 계산법을 코사인 유사도로 튜닝한 Spherical K-means를 구현하여 적용 
  6. 군집 분석 및 알고리즘 구축 성공


### Reference
![image](https://user-images.githubusercontent.com/85140314/140265679-2ba71175-8d78-4626-9515-c52017d78090.png)
