from django.contrib.auth.models import User,Group
from django.views.generic import ListView
from rest_framework import viewsets, exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Product
from .serializers import UserSerializer,GroupSerializer,ProductSerializer

import csv,json

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
        response = {'query':query,'find':'[1,2,]'}
        #response메시지를 만들어야함
        return Response(response,status=200)


class ProductViewSet(APIView):
    def get(self,request):
        model=Product
        queryset = Product.objects.all()
        #queryset.delete()
        #print("model objects 다 지우기")

        bulk_list=[]
        #Product 모델이 비었을때 데이터셋 bulk crate
        if queryset.exists() == False:
            with open("C:\\Users\\gg664\\IdeaProjects\\Bigdata_Study\\NewSearch\\mysite\\search\\data\\test_data.csv","r", encoding='UTF8') as raw_file:
                reader = csv.reader(raw_file,delimiter ='\t')

                next(reader) # header 생략
                for row in reader:
                    bulk_list.append(Product(id=row[0],name=row[1]))

            Product.objects.bulk_create(bulk_list)
            print("product에 bulk 완료")

        print("이미 product에 data 있음",Product.objects.all())
        serializer_class = ProductSerializer(queryset,many=True)

        return Response(serializer_class.data,status=200)

    def get_list(self,request):
        return Product.objects.all()
