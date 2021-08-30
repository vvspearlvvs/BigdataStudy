import re,json

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pandas as pd

from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer

import os
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = BASE_PATH+'/data/'

data_filename = 'test_data.csv'
index_filename = 'test_index.parquet'
tifid_filename ='test_tifid.parquet'

def tokenizer(data):
    token=[]

    data = data.lower() #소문자로 변환
    words = data.split() #공백으로 분리

    #규칙
    '''
    - 공백 기준으로 분리
    - 영어는 소문자로 변환
    - 공백으로 분리된 텍스트 안에서 다음 규칙으로 토큰을 구분
    - 연속된 한글 : [가-힣]+
    - 연속된 자모 : [ㄱ-ㅎ|ㅏ-ㅣ]+
    - 연속된 영문, 숫자, 하이픈(-) : [a-z0-9-]+
    - 그 외 문자는 묶어서 하나로 취급 : [^ a-z0-9가-힣+] (연속된 특수문자)
    '''
    pattern = re.compile("[가-힣]+|[ㄱ-ㅎ|ㅏ-ㅣ]+|[a-z0-9-]+|[^ a-z0-9가-힣+]")

    for word in words:
        find = re.findall(pattern,word)
        for w in find:
            token.append(w)
    return token

def data_load():
    # parquet포맷으로 read csv
    table = pc.read_csv(DATA_PATH+data_filename,parse_options=pc.ParseOptions(delimiter="\t"))
    df=table.to_pandas()
    df['token'] = df['name'].apply(tokenizer) #token 컬럼생성

    #print(df)
    '''
    id	name	    token
    0	153709120	갤럭시 중고폰	[갤럭시, 중고폰]
    1	153709376	연예인 지갑 지갑	[연예인, 지갑, 지갑]
    2	153710656	갤럭시s10 갤럭시북 NT950XDZ-G58AW 중고폰	[갤럭시, s10, 갤럭시북, nt950xdz-g58aw, 중고폰]
    '''
    return df

def data_json(df):
    js = df.to_json(orient = 'records') #row기반
    json_data =json.loads(js)

    #print(json_data)
    '''
    [   {'id': 153709120, 'name': '갤럭시 중고폰', 'token': ['갤럭시', '중고폰']},
        {'id': 153709376, 'name': '연예인 지갑 지갑', 'token': ['연예인', '지갑', '지갑']},
        {'id': 153710656,'name': '갤럭시s10 갤럭시북 NT950XDZ-G58AW 중고폰','token': ['갤럭시', 's10', '갤럭시북', 'nt950xdz-g58aw', '중고폰']}
    ]
    '''
    return json_data

def save_invertedInex(json_data):
    index_dict=defaultdict(list) #inverted index생성, 같은문장에 같은단어 중복출현 set()

    for data in json_data:
        for token in data['token']:
            index_dict[token].append(data['id'])

    #print(index_dict)
    '''
    defaultdict(<class 'list'>, 
    {'갤럭시': [153679680, 153682496], '중고폰': [153679680, 153682496], '지갑': [153680960, 153680960], 's10': [153682496], .. ]})
    '''

    index_df = pd.DataFrame(index_dict.items(),columns=['token', 'docu_list'])
    #print(index_df)
    '''
        token               docu_list
    0             갤럭시  [153709120, 153710656]
    1             중고폰  [153709120, 153710656]
    2             연예인             [153709376]
    '''

    #parquet파일 생성
    table = pa.Table.from_pandas(index_df)
    pq.write_table(table, DATA_PATH+index_filename)


def save_tfidf(df):
    vect2 = TfidfVectorizer(tokenizer=tokenizer,max_features=10) #가장 많이 나온 단어 N개만 사용

    tfvect_matrix = vect2.fit_transform(df['name']) #tf-idf 계산
    tfidf_col = vect2.get_feature_names()

    tfidv_df = pd.DataFrame(tfvect_matrix.toarray(), index = list(df['id']), columns = sorted(tfidf_col)) #tf-idf적용 df생성
    #print(tfidv_df)
    ''''
                 nt950xdz-g58aw	 s10	   갤럭시	갤럭시북	    연예인	    중고폰	    지갑
    153709120	0.000000	0.000000	0.707107	0.000000	0.000000	0.707107	0.000000
    153709376	0.000000	0.000000	0.000000	0.000000	0.447214	0.000000	0.894427
    153710656	0.490479	0.490479	0.373022	0.490479	0.000000	0.373022	0.000000
    '''

    #parquet파일 생성
    table = pa.Table.from_pandas(tfidv_df)
    pq.write_table(table, DATA_PATH+tifid_filename)

def search_response(df,query_documents,tfidv_df,token_list):
    #교집합 문서들의 id,name
    search_dc = df[df['id'].isin(query_documents)]
    search_dc=search_dc.set_index('id')

    #교집합 문서들에 대해서 tf-dif(score값)
    #tfidv_df.loc[query_documents] #교집합문서
    search_tf = tfidv_df.loc[query_documents][token_list]
    search_tf['score'] = search_tf.sum(axis=1)

    # search_dc와 search_tf join (by id)
    search=search_tf.join(search_dc,how='inner')
    search['pid']=search.index
    search.sort_values(by=['score'],ascending=[False],inplace=True) #score기준 정렬

    # response msg
    response = search[['pid','name','score']]
    js = response.to_json(orient='records')
    res_data =json.loads(js)

    return res_data