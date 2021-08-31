import re,json

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pandas as pd

from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer

import os
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

'''
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
    def save_invertedIndex(self, df):

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

## 검색구현
class Mathching:

    ## invertIndex로 매칭
    def match_invertedInex(self,index_df,token_list):

        # invertIndex dataframe 사용해서 매칭되는 문서ID리스트검색
        search_token =index_df[index_df['token'].isin(token_list)]
        matching_list = search_token['docu_list']


        tmp_doculist = [set(docu_list) for docu_list in matching_list]
        intersect_doculist = list(tmp_doculist[0].intersection(*tmp_doculist))

        # invertIndex에 있는 token list
        check_token = list(search_token['token'])

        '''
        # 예외. invertIndex의 token에 없는 키워드로 검색한 경우
        if len(check_token) == 0:
            response = {'exception' : 'invertIndex에 없는 키워드로 검색했습니다'}
            return Response(response,status=200)
        '''

        return intersect_doculist

    ## tf-idf로 score
    def match_tfidf(self,df):
        vect = TfidfVectorizer(tokenizer=tokenizer)
        tfvect_matrix = vect.fit_transform(df['name'])
        tfidf_col = vect.get_feature_names()

        # TF-IDF통계값 dataframe생성
        tfidv_df = pd.DataFrame(tfvect_matrix.toarray(), index = df.index, columns = sorted(tfidf_col)) #tf-idf적용 df생성
        tfidv_df['score'] = tfidv_df.sum(axis=1)
        tfidv_df.sort_values(by=['score'],ascending=[False],inplace=True) #score기준 정렬
        tfidv_df = tfidv_df[:20]

        return tfidv_df

    def match_result(self,data_df,intersect_doculist):
        search_tf = data_df[data_df['id'].isin(intersect_doculist)]
        search_tf = search_tf.set_index('id')
        result_name = search_tf['name'] #id,name 검색결과

        tfidf_df = self.match_tfidf(search_tf)
        result_score = tfidf_df['score'] #id,score 검색결과

        result = pd.concat([result_name,result_score],axis=1)
        result.reset_index(inplace=True)

        return result