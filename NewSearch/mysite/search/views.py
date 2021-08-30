from django.contrib.auth.models import User,Group
from django.views.generic import ListView
from rest_framework import viewsets, exceptions, status
from rest_framework.response import Response
from rest_framework.views import APIView

from .serializers import *

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

        # model에 없을때만 data load, index 생성
        prd_queryset = Product.objects.all()
        prd_queryset.delete()

        if prd_queryset.exists() == False:

            df = data_load()

            # product model에 data 넣기 -> 굳이?
            product_obj = [Product(id=row['id'],name=row['name'],token=row['token']) for i,row in df.iterrows()]
            Product.objects.bulk_create(product_obj)

            # index parquet 파일생성
            json_data = data_json(df)
            save_invertedInex(json_data)

            # tf-idf parquet 파일생성
            save_tfidf(df)

        #이미 load한 상태는 query 처리
        query = request.GET['query']
        print("검색어",query)
        token_list = tokenizer(query).split()
        print("토큰결과",token_list)

        # 예외사항 : token에 해당하는 이름이 아니면 오류

        #index file 가져오기
        index_file ='C:\\Users\\gg664\\IdeaProjects\\Bigdata_Study\\NewSearch\\mysite\\search\\data\\test_index.parquet'
        index_df = pq.read_table(index_file).to_pandas()
        #print("index file",index_df)


        #토큰결과를 가지고 revered_index에서 documnet list찾아옴 -> 다시 행으로 검색해야함..
        q_documents=[]
        search_token = index_df[index_df['token'].isin(token_list)]
        for docu_list in search_token['docu_list']:
            q_documents.append(set(docu_list))
            #print(q_documents)

        # 문서들의 교집합
        query_documents = list(q_documents[0].intersection(*q_documents))
        #print(query_documents)

        #tf-idf file 가져오기
        #index file 가져오기
        tfidf_file ='C:\\Users\\gg664\\IdeaProjects\\Bigdata_Study\\NewSearch\\mysite\\search\\data\\test_tfidf.parquet'
        tfidf_df = pq.read_table(tfidf_file).to_pandas()
        #print("index file",index_df)

        #최종결과
        search_result = search_response(df,query_documents,tfidf_df,token_list)

       #response message
        response = {'query':query,'data':search_result}

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