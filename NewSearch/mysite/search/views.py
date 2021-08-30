from django.contrib.auth.models import User,Group
from django.views.generic import ListView
from rest_framework import viewsets, exceptions, status
from rest_framework.response import Response
from rest_framework.views import APIView


from .models import Product
from .serializers import *
from django_elasticsearch_dsl_drf.viewsets import DocumentViewSet

from django_elasticsearch_dsl_drf.filter_backends import FilteringFilterBackend,CompoundSearchFilterBackend

from elasticsearch import Elasticsearch
import csv,json,re

from .processing import *

class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all().order_by('-date_joined')
    serializer_class = UserSerializer


class GroupViewSet(viewsets.ModelViewSet):
    queryset = Group.objects.all()
    serializer_class = GroupSerializer

class Search(APIView):
    def get(self,request):
        if not 'query' in request.GET:
            raise exceptions.ParseError("검색어를 입력해주세요")

        query = request.GET['query']
        print("검색어",query)

        token_list = tokenizer(query).split()
        print("토큰결과",token_list)

        #토큰결과를 가지고 revered_index에서 documnet list찾아옴
        df = data_load()
        json_data = data_json(df)
        invert_index = get_invertedInex(json_data)
        tfidf = get_tfidf(df)


        #query가 있는 문서id 찾기
        q_documents=[]
        for tk in token_list:
            q_documents.append(set(invert_index[tk]))

        # 문서들의 교집합
        query_documents = list(q_documents[0].intersection(*q_documents))
        print("문서들의 교집합",query_documents)

        #교집합 문서들의 id,name
        search_dc = df[df['id'].isin(query_documents)]
        search_dc=search_dc.set_index('id')

        #교집합 문서들에 대해서 tf-dif(score값)
        #tfidv_df.loc[query_documents] #교집합문서
        search_tf = tfidf.loc[query_documents][token_list]
        search_tf['score'] = search_tf.sum(axis=1)

        # search_dc와 search_tf join (by id)
        search=search_tf.join(search_dc,how='inner')
        search['pid']=search.index
        search.sort_values(by=['score'],ascending=[False],inplace=True) #score기준 정렬
        print("검색결과",search)

        response = search[['pid','name','score']]

        # response msg
        js = response.to_json(orient='records')
        res_data =json.loads(js)


       #response message
        response = {'query':query,'data':res_data}

        return Response(response,status=200)

#raw data 그냥 보여주는 view
class ProductViewSet(APIView):
    def get(self,request):
        model=Product
        queryset = Product.objects.all()
        #queryset.delete()
        #print("model objects 다 지우기")

        bulk_list=[]
        #데이터셋 bulk create
        if queryset.exists() == False:
            with open("C:\\Users\\gg664\\IdeaProjects\\Bigdata_Study\\NewSearch\\mysite\\search\\data\\test_data.csv","r", encoding='UTF8') as raw_file:
                reader = csv.reader(raw_file,delimiter ='\t')

                next(reader) # header 생략
                for row in reader:
                    bulk_list.append(Product(id=row[0],name=row[1]))

            Product.objects.bulk_create(bulk_list)
            print("product에 bulk 완료")

        #데이터셋 직렬화
        print("bulk data 확인",Product.objects.all())
        serializer_class = ProductSerializer(queryset,many=True)

        return Response(serializer_class.data,status=200)