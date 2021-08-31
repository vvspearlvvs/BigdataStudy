from django.http import HttpResponse

from rest_framework import viewsets, exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

import os
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

'''
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


from .serializers import *
from .processing import *
etl = DataSetting()
match = Mathching()

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

        # 검색어를 Tokenizer 로 각각 분리
        query = request.GET['query']
        token_list = tokenizer(query)
        print("검색어: ",query, ", 분리: ", token_list)

        '''
        ddtaload 이슈 방법2. 로컬에 tsv file read/write
        -> 초기세팅을 위한 파일생성 1분소요, 세잍완료된 상태에선 10초 소요
        
        변경예정
        dataload 이슈방법3. s3에 tsv file 넣어둔 상태에서 시작 
        s3 bucket에 parquet file이 없을때 초기세팅
        
        '''
        # 초기세팅(etl) : 로컬 디렉토리에 parquet file이 없을 때 진행
        dir = os.listdir(DATA_PATH)
        if len(dir) == 1:
            print("초기세팅")
            raw_df = etl.data_load()
            etl.save_invertedIndex(raw_df)
            etl.save_tfidf(raw_df)

        # 초기세팅이 이전에 진행된 경우 : parquet file에서 가져와서 매칭진행
        data_df = pq.read_table(DATA_PATH+data_filename).to_pandas()
        index_df = pq.read_table(DATA_PATH+index_filename).to_pandas()
        #tfidf_df = pq.read_table(DATA_PATH+tfdif_filename).to_pandas()

        # invertIndex로 매칭되는 문서ID리스트들의 교집합 가져옴
        matching_doculist = match.match_invertedInex(index_df,token_list)
        print("매칭되는 문서ID리스트 교집합 ",len(matching_doculist))

        # 각 문서에 대하여 TF-IDF를 이용해서 score계산
        result = match.match_result(data_df,matching_doculist)
        js = result.to_json(orient='records')
        search_result =json.loads(js)

        # 예외. 교집합 문서ID가 없는 경우
        if not matching_doculist:
            response = {'exception' : '교집합 문서ID가 없가 존재하지 않습니다'}
            return Response(response,status=200)

        #response message
        response = {'data':search_result}
        return Response(response,status=200)