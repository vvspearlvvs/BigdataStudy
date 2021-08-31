import re,json

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pandas as pd

from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer

import os
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

#testdata.ver
DATA_PATH = BASE_PATH+'/test_data/'
raw_data = 'test_data.csv'

data_filename = 'data.parquet'
index_filename = 'test_index.parquet'
tfdif_filename = 'test_tfidf.parquet'

'''
#realdata.ver
raw_data = 'prodct_name.tsv'

DATA_PATH = BASE_PATH+'/data/'
data_filename = 'data.parquet'
index_filename = 'index.parquet'
tfdif_filename = 'tfidf.parquet'
'''

def tokenizer(data):
    token=[]

    data = data.lower() #소문자로 변환
    words = data.split() #공백으로 분리

    pattern = re.compile("[가-힣]+|[ㄱ-ㅎ|ㅏ-ㅣ]+|[a-z0-9-]+|[^ a-z0-9가-힣+]")

    for word in words:
        find = re.findall(pattern,word)
        for w in find:
            token.append(w)
    return token

def data_load():
    # parquet포맷으로 read csv
    table = pc.read_csv(DATA_PATH+raw_data,parse_options=pc.ParseOptions(delimiter="\t"))
    df=table.to_pandas()
    df['token'] = df['name'].apply(tokenizer) #token 컬럼생성

    #parquet파일 생성
    table = pa.Table.from_pandas(df)
    pq.write_table(table, DATA_PATH+data_filename)

    return df

def data_json(df):
    js = df.to_json(orient = 'records') #row기반
    json_data =json.loads(js)

    return json_data

def save_invertedInex(json_data):
    index_dict=defaultdict(list) #inverted index생성, 같은문장에 같은단어 중복출현 set()

    for data in json_data:
        for token in data['token']:
            index_dict[token].append(data['id'])

    index_df = pd.DataFrame(index_dict.items(),columns=['token', 'docu_list'])

    #parquet파일 생성
    table = pa.Table.from_pandas(index_df)
    pq.write_table(table, DATA_PATH+index_filename)

def save_tfidf(df):
    vect2 = TfidfVectorizer(tokenizer=tokenizer,max_features=100) #가장 많이 나온 단어 N개만 사용

    tfvect_matrix = vect2.fit_transform(df['name']) #tf-idf 계산
    tfidf_col = vect2.get_feature_names()

    tfidv_df = pd.DataFrame(tfvect_matrix.toarray(), index = list(df['id']), columns = sorted(tfidf_col)) #tf-idf적용 df생성

    #parquet파일 생성
    table = pa.Table.from_pandas(tfidv_df)
    pq.write_table(table, DATA_PATH+tfdif_filename)

def search_response(df,query_documents,tfidv_df,token_list):
    #교집합 문서들의 id,name
    search_dc = df[df['id'].isin(query_documents)]
    search_dc=search_dc.set_index('id')

    #교집합 문서들에 대해서 tf-dif(score값)
    #tfidv_df.loc[query_documents] #
    tf_token_list =[]
    for token in token_list:
        if token in tfidv_df.columns:
            tf_token_list.append(token)

    search_tf = tfidv_df.loc[query_documents][tf_token_list]
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