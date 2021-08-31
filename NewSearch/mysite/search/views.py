from django.http import HttpResponse

from rest_framework import viewsets, exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

import os
BASE_PATH = os.path.dirname(os.path.abspath(__file__))


#testdata.ver
DATA_PATH = BASE_PATH+'/test_data/'
data_filename = 'data.parquet'
index_filename = 'test_index.parquet'
tfdif_filename = 'test_tfidf.parquet'

'''
#realdata.ver
DATA_PATH = BASE_PATH+'/data/'
data_filename = 'data.parquet'
index_filename = 'index.parquet'
tfdif_filename = 'tfidf.parquet'
'''

from .serializers import *
from .processing import *
etl = DataSetting()

class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all().order_by('-date_joined')
    serializer_class = UserSerializer


class GroupViewSet(viewsets.ModelViewSet):
    queryset = Group.objects.all()
    serializer_class = GroupSerializer

## 검색API 구현
class Search(APIView):
    def get(self,request):

        # 예외. 잘못된 request형식
        if not 'query' in request.GET:
            raise exceptions.ParseError("Query Parameter Sample : /search?query=아이폰 ")

        ### dtaload 이슈 방법2. 로컬에 tsv file read/write
        ### -> 초기세팅을 위한 파일생성 1분소요, 세잍완료된 상태에선 10초 소요

        #  디렉토리에 parquet file이 없을때 초기세팅
        dir = os.listdir(DATA_PATH)
        if len(dir) == 1:
            print("초기세팅")
            raw_df = etl.data_load()

            # index parquet 파일생성
            etl.save_invertedInex(raw_df)

            # tf-idf parquet 파일생성
            etl.save_tfidf(raw_df)

        ### 이미 load한 상태 : dataload 이슈 방법2
        #data file 가져오기
        data_df = pq.read_table(DATA_PATH+data_filename).to_pandas()
        #index file 가져오기
        index_df = pq.read_table(DATA_PATH+index_filename).to_pandas()
        #tf-idf file 가져오기
        tfidf_df = pq.read_table(DATA_PATH+tfdif_filename).to_pandas()

        ### dataload 이슈방법3. s3에 tsv file 넣어둔 상태에서 시작
        # s3 bucket에 parquet file이 없을때 초기세팅

        #이미 load한 상태
        print("이미 세팅된 상태")
        query = request.GET['query']
        token_list = tokenizer(query)
        print("검색어 ",query, "검색어 token", token_list)

        #토큰결과를 가지고 revered_index에서 documnet list 검색 -> 다시 행으로 검색해야함..
        search_token = index_df[index_df['token'].isin(token_list)]
        new_token_list = list(search_token['token'])

        #없는 키워드로 검색할 경우
        if len(new_token_list) == 0:
            raise exceptions.ParseError("없는 키워드입니다")

        #documnet list 검색
        q_documents=[]
        for docu_list in search_token['docu_list']:
            q_documents.append(set(docu_list))
        # 문서들의 교집합
        query_documents = list(q_documents[0].intersection(*q_documents))
        print("문서들의 교집합 개수", len(query_documents))

        # 교집합이 없는 경우
        if not query_documents:
            raise exceptions.ParseError("두 키워드의 교집합이 존재하지 않습니다")

        #최종결과
        search_result = search_response(data_df,query_documents,tfidf_df,new_token_list)

       #response message
        response = {'data':search_result}

        return Response(response,status=200)