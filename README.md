# BigdataStudy
Airflow, 추천시스템, 데이터전처리, EDA 등 데이터관련 개인학습 및 실습저장소

## NewSearch 

### 개요

약 10만건의 상품명 데이터를 가공 및 처리하여 검색API 및 검색엔진 Django 웹 개발<br>

### 사용기술

- Language : Python 3.6  <br>
- Packages : sklearn의 TfidfVectorizer,  pandas, pyarrow
- Web : Django restframework  <br>
- 데이터 : 약 10만건의 sample데이터 (보안상 저장X)

### 수행역할

대용량 데이터를 기반으로 ETL프로세스 수행하여 검색API구현  

- 텍스트 데이터 전처리 수행
- TF-IDF기반의 피쳐벡터화 구현
- 검색엔진 Django 웹 개발

[troubleshooting 기록문서](./NewSearch/진행과정.md)

- 대용량의 데이터의 read 속도향상 :  pyarrow 사용
- 대용량의 데이터를 load 속도향상 : parquet 파일저장
- 키워드 검색 효율향상  : tokenize 및 invertindex 생성
- TF-IDF기반의 피쳐벡터화 수행시 메모리 효율향상  : 로직순서 변경

### 수행과정

![images](./NewSearch/Test/images/images.png)

- 초기검색
  - data load : /data/proct_name.tsv 파일을 pyarrow로 읽어서 data.parquet로 저장
  - invertIndex : 구현한 tokenizer로 분리된 token이 key, 문서id리스트가 value를 index.parquet로 저장
- 검색
  - index.parquet을 읽서 dataframe(index_df)로 사용 -> 매칭되는 문서ID리스트의 교집합ID검색
  - data.parquet을 읽어 dataframe(data_df)로 사용 -> 검색된 교집합ID의 id,name 저장
  - 사이킷런의 TfidfVectorizer 사용해서 dataframe(tfidf_df)로 사용 -> 검색된 교집합ID의 id,score 저장
  - Response : score기준으로 정렬된 상위20개 검색결과(id,name,score)

### 프로젝트결과

![result](./NewSearch/Test/images/test_result.png)

- 한계점 및 보완할점
  - 초기세팅(data load) 최대1분 소요, 하지만 parquet파일이 load 된 상태라면 검색빠름
  - 로컬파일 : parquet파일들은 내부 로컬리렉토리에서 read/write진행
  - 더 효율적인 방법이 있는지 고민해보기 : 로컬파일이 아니라 s3에 저장하기? spark로 처리하기?



