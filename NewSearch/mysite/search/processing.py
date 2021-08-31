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

## Tokenizer구현
def tokenizer(data):
    token=[]

    data = data.lower() # 영어소문자로 변환
    words = data.split() # 공백기준으로 분리

    # 규칙기반 분리
    pattern = re.compile("[가-힣]+|[ㄱ-ㅎ|ㅏ-ㅣ]+|[a-z0-9-]+|[^ a-z0-9가-힣+]")

    # 공백으로 분리된 텍스트안에서 규칙으로 tokenlist생성
    for word in words:
        find = re.findall(pattern,word)
        for w in find:
            token.append(w)
    return token

## 초기세팅구현
class DataSetting:

    ## rawdata(tsv file) 수집/저장
    def data_load(self):

        # pyarrow로 tsv파일 read
        table = pc.read_csv(DATA_PATH+raw_data,parse_options=pc.ParseOptions(delimiter="\t"))

        # dataframe생성 : name을 토큰화하여 'token'컬럼추가
        df=table.to_pandas()
        df['token'] = df['name'].apply(tokenizer)

        # data.parquet 파일저장
        table = pa.Table.from_pandas(df)
        pq.write_table(table, DATA_PATH+data_filename)

        return df

    ## invertIndex 생성/저장
    def save_invertedInex(self, df):

        # invertIndex생성준비 : dataframe을 row기반으로 변환
        js = df.to_json(orient = 'records') #row기반
        json_data =json.loads(js)

        # invertIndex dict생성 : 분리된 token이 key, 문서id리스트가 value
        index_dict=defaultdict(list)
        for data in json_data:
            for token in data['token']:
                index_dict[token].append(data['id'])

        # invertIndex dataframe생성
        index_df = pd.DataFrame(index_dict.items(),columns=['token', 'docu_list'])

        # index.parquet 파일저장
        table = pa.Table.from_pandas(index_df)
        pq.write_table(table, DATA_PATH+index_filename)

    ## TF-IDF통계값 생성/저장
    def save_tfidf(self, df):

        # TfidfVectorizer사용 : features의 토큰화기준은 직접구현한 tokenizer사용
        '''
            문제 : 로컬 python의 메모리부족
            원인 :  vect = TfidfVectorizer(tokenizer=tokenizer)에서 메모리부족
            임시해결 : 가장 많이나온단어(max_features) 기준으로 100개 features 사용
            임시해결 : 없는 features는 tf-idf socre 합계에 포함되지 않음
        '''
        vect = TfidfVectorizer(tokenizer=tokenizer,max_features=100)
        tfvect_matrix = vect.fit_transform(df['name'])
        tfidf_col = vect.get_feature_names()

        # TF-IDF통계값 dataframe생성
        tfidv_df = pd.DataFrame(tfvect_matrix.toarray(), index = list(df['id']), columns = sorted(tfidf_col)) #tf-idf적용 df생성

        # tfidf.parquet 파일저장
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