# SNS_NLP_PJT
Natural Language Processing Project based on SNS text data  

# 프로젝트명 : 코로나시대, 안전한 여행을 위한 SNS기반 감성숙소 추천서비스
## 프로젝트 개요
  - 코로나 시대 속 안전하고 다양한 여행을 위한 숙소 정보 제공 및 추천 서비스를 구현함.
### 활용 데이터 
  - 인스타그램 감성숙소 정보공유 게시글 & 네이버 블로그 리뷰 데이터
### 활용 기술
  - 웹 크롤링, Kiwi, Word2Vec, K-means, Content based Filltering, HDFS, MongoDB 
### 분석 방식
  1. 크롤링을 통한 데이터 수집
  2. 1차 전처리(특수문자 제거, 문자열 합치기 등)
  3. Kiwi 형태소 분석기를 통한 토큰화
  4. 2차 전처리(불용어 처리, 공백 제거 등)
  5. EDA / W2v을 활용한 Vectorization 
  6. Spherical K-means Clustering
  7. 코사인 유사도를 적용한 아이템 기반 필터링 알고리즘 구축
  8. 군집 분석을 기반으로 한 추론 알고리즘 구축

