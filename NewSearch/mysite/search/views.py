from django.http import HttpResponse

from rest_framework import viewsets, exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

import os
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

'''
#testdata.ver
DATA_PATH = BASE_PATH+'/test_data/'
data_filename = 'test_data.parquet'
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

        # 초기세팅(etl) : 로컬 디렉토리에 parquet file이 없을 때 진행
        dir = os.listdir(DATA_PATH)
        if len(dir) == 1:
            print("초기세팅 (최대1분소요) ")
            raw_df = etl.data_load()
            etl.save_invertedIndex(raw_df)

        # 초기세팅이 이전에 진행된 경우 : parquet file에서 가져와서 매칭진행
        data_df = pq.read_table(DATA_PATH+data_filename).to_pandas()
        index_df = pq.read_table(DATA_PATH+index_filename).to_pandas()

        #예외. invertIndex의 token에 없는 키워드로 검색한 경우
        if len(token_list) == 1 and token_list not in list(index_df['token']):
            response = {'exception' : 'invertIndex에 없는 키워드로 검색했습니다'}
            return Response(response,status=200)

        # invertIndex로 매칭되는 문서ID리스트들의 교집합 가져옴
        matching_doculist = match.match_invertedInex(index_df,token_list)
        print("매칭되는 문서ID리스트들의 교집합", len(matching_doculist))

        # 예외. 교집합 문서ID가 없는 경우
        if not matching_doculist:
            response = {'exception' : '교집합 문서ID가 없가 존재하지 않습니다'}
            return Response(response,status=200)

        # 각 문서에 대하여 TF-IDF를 이용해서 score계산
        result = match.match_result(data_df,matching_doculist)
        top20_result = result[:20] #상위20

        # response msg생성
        js = top20_result.to_json(orient='records')
        search_result =json.loads(js)
        response = {'data':search_result}
        return Response(response,status=200)