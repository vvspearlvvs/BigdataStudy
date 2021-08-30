from django.test import TestCase
import os
import pandas as pd
import pyarrow.csv as pycsv

import re,json,csv
from collections import defaultdict

from sklearn.feature_extraction.text import TfidfVectorizer

# Create your tests here.
pd.set_option("display.width",300)
pd.set_option("display.max_rows",1000)
pd.set_option("display.max_columns",30)

def etc():
    with open("data/prodct_name.tsv", encoding='UTF8') as raw_file:
        reader = csv.reader(raw_file,delimiter ='\t')
        next(reader)
        #for row in reader:
           #print(row)
    
    json_data=[]
    with open("C:\\Users\\gg664\\IdeaProjects\\Bigdata_Study\\NewSearch\\mysite\\search\\data\\test_data.csv", encoding='UTF8') as raw_file:
        reader = csv.reader(raw_file,delimiter ='\t')

        column_names = next(reader) # header 생략

        for i,cols in enumerate(reader):
            raw = {col_name:col for col_name, col in zip(column_names,cols)}
            raw['token']=tokenizer(raw['name'])
            json_raw = json.dumps(raw, ensure_ascii=False)

            #body = body + json.dumps({"index": {"_index": "product", "_id":i}}) + '\n'
            #body = body + json_raw + '\n'
            json_data.append(json_raw)
    print(json_data)
    
    index_dict=defaultdict(list)

    for data in json_data:
        print(data["token"])
        
        for token in data['token'].split():
            if token in data['token']:
                if token in index_dict:
                    index_dict[token].append(data['id'])
                else:
                    index_dict[token]=[data['id']]
    print(index_dict)

def test_data():
    test_data = [[10,'갤럭시 중고폰'],[11,'연예인 지갑 중고폰'],[12,'갤럭시s10 갤럭시북 NT950XDZ-G58AW 지갑']]
    test_df = pd.DataFrame(test_data, columns=['id','name'])
    return test_df

def tokenizer(data):
    #token=[]
    token = ''

    data = data.lower() #소문자로 변환

    words = data.split() #공백으로 분리
    #print("split ", words)

    #규칙에 해당 -> findall
    '''
    - 공백 기준으로 분리
    - 영어는 소문자로 변환
    - 공백으로 분리된 텍스트 안에서 다음 규칙으로 토큰을 구분
    - 연속된 한글 : [가-힣]+
    - 연속된 자모 : [ㄱ-ㅎ|ㅏ-ㅣ]+
    - 연속된 영문, 숫자, 하이픈(-) : [a-zA-Z0-9-]+
    - 그 외 문자는 묶어서 하나로 취급 : [^ A-Za-z0-9가-힣+] (연속된 특수문자)
    '''
    p = re.compile("[가-힣]+|[ㄱ-ㅎ|ㅏ-ㅣ]+|[0-9a-z]+|[a-z0-9-]+|[^ a-z0-9가-힣+]") #규칙

    for word in words:
        find = re.findall(p,word)
        for w in find:
            token=token+w+' '
            #token.append(w)
    return token


def data_load():
    table = pycsv.read_csv("data/test_data.csv",parse_options=pycsv.ParseOptions(delimiter="\t")) #파케이포맷 -> pandas
    df=table.to_pandas()

    df['token'] = df['name'].apply(tokenizer) #token 컬럼생성
    print(df)
    '''
              id                            name                              token
    0  153679680                         갤럭시 중고폰                           갤럭시 중고폰 
    1  153680960                       연예인 지갑 지갑                         연예인 지갑 지갑 
    2  153682496  갤럭시s10 갤럭시북 NT950XDZ-G58AW 중고폰  갤럭시 s10 갤럭시북 nt950xdz -g58aw 중고폰 
    '''
    return df


def data_json(df):
    js = df.to_json(orient = 'records') #row기반
    json_data =json.loads(js)

    print(json_data)
    '''
    [   {'id': 153679680, 'name': '갤럭시 중고폰', 'token': '갤럭시 중고폰 '},
        {'id': 153680960, 'name': '연예인 지갑 지갑', 'token': '연예인 지갑 지갑 '}, 
        {'id': 153682496, 'name': '갤럭시s10 갤럭시북 NT950XDZ-G58AW 중고폰', 'token': '갤럭시 s10 갤럭시북 nt950xdz -g58aw 중고폰 '}
    ]
    '''

    return json_data

def get_invertedInex(json_data):
    index_dict=defaultdict(list) #inverted index생성

    for data in json_data:
        for token in data['token'].split():
            if token in data['token']:
                if token in index_dict:
                    index_dict[token].append(data['id'])
                else:
                    index_dict[token]=[data['id']]

    print(index_dict)
    '''
    defaultdict(<class 'list'>, 
    {'갤럭시': [153679680, 153682496], '중고폰': [153679680, 153682496], '지갑': [153680960, 153680960], 's10': [153682496], .. ]})
    '''

    return index_dict

def get_tfidf(df):
    vect2 = TfidfVectorizer(max_features=10) #가장 많이 나온 단어 N개만 사용

    tfvect_matrix = vect2.fit_transform(df['token']) #tf-idf 계산
    tfidf_col = vect2.get_feature_names() #유니크한 단어수 --> 문제 : 단어로 짤라서,-g58aw의 경우 -가 짤림!!!!!

    tfidv_df = pd.DataFrame(tfvect_matrix.toarray(), columns = sorted(tfidf_col)) #tf-idf적용 df생성
    tfidv_df.index = df['id']
    print(tfidv_df)
    ''''
                  g58aw  nt950xdz    s10      갤럭시     갤럭시북    연예인      중고폰      지갑
    id                                                                                       
    153679680  0.000000  0.000000  0.000000  0.707107  0.000000  0.000000  0.707107  0.000000
    153680960  0.000000  0.000000  0.000000  0.000000  0.000000  0.447214  0.000000  0.894427
    153682496  0.440362  0.440362  0.440362  0.334907  0.440362  0.000000  0.334907  0.000000
    '''
    return tfidv_df

if __name__=='__main__':
    df = data_load()
    json_data = data_json(df)
    invert_index = get_invertedInex(json_data)
    tfidf = get_tfidf(df)
