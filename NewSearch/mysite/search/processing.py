import re,json

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pandas as pd

from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer

OSPAHT=''

def tokenizer(data):
    #token=[]
    token = ''

    data = data.lower() #소문자로 변환
    words = data.split() #공백으로 분리

    #규칙
    '''
    - 공백 기준으로 분리
    - 영어는 소문자로 변환
    - 공백으로 분리된 텍스트 안에서 다음 규칙으로 토큰을 구분
    - 연속된 한글 : [가-힣]+
    - 연속된 자모 : [ㄱ-ㅎ|ㅏ-ㅣ]+
    - 연속된 영문, 숫자, 하이픈(-) : [a-zA-Z0-9-]+
    - 그 외 문자는 묶어서 하나로 취급 : [^ A-Za-z0-9가-힣+] (연속된 특수문자)
    '''
    p = re.compile("[가-힣]+|[ㄱ-ㅎ|ㅏ-ㅣ]+|[0-9a-z]+|[a-z0-9-]+|[^ a-z0-9가-힣+]")

    for word in words:
        find = re.findall(p,word)
        for w in find:
            token=token+w+'\n'
            #token.append(w)
    return token

def data_load():
    # parquet포맷으로 read csv
    table = pc.read_csv("C:\\Users\\gg664\\IdeaProjects\\Bigdata_Study\\NewSearch\\mysite\\search\\data\\test_data.csv",parse_options=pc.ParseOptions(delimiter="\t"))
    df=table.to_pandas()

    df['token'] = df['name'].apply(tokenizer) #token 컬럼생성
    #print(df)
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

    #print(json_data)
    '''
    [   {'id': 153679680, 'name': '갤럭시 중고폰', 'token': '갤럭시 중고폰 '},
        {'id': 153680960, 'name': '연예인 지갑 지갑', 'token': '연예인 지갑 지갑 '}, 
        {'id': 153682496, 'name': '갤럭시s10 갤럭시북 NT950XDZ-G58AW 중고폰', 'token': '갤럭시 s10 갤럭시북 nt950xdz -g58aw 중고폰 '}
    ]
    '''

    return json_data
def save_invertedInex(json_data):
    index_dict=defaultdict(list) #inverted index생성, 같은문장에 같은단어 중복출현 set()

    for data in json_data:
        for token in data['token'].split():
            index_dict[token].append(data['id'])

    #print(index_dict)
    '''
    defaultdict(<class 'list'>, 
    {'갤럭시': [153679680, 153682496], '중고폰': [153679680, 153682496], '지갑': [153680960, 153680960], 's10': [153682496], .. ]})
    '''

    index_df = pd.DataFrame(index_dict.items(),columns=['token', 'docu_list'])
    print(index_df)

    table = pa.Table.from_pandas(index_df)
    pq.write_table(table, 'C:\\Users\\gg664\\IdeaProjects\\Bigdata_Study\\NewSearch\\mysite\\search\\data\\test_index.parquet')


def save_tfidf(df):
    vect2 = TfidfVectorizer(max_features=10) #가장 많이 나온 단어 N개만 사용

    tfvect_matrix = vect2.fit_transform(df['token']) #tf-idf 계산
    tfidf_col = vect2.get_feature_names() #유니크한 단어수 --> 문제 : 단어로 짤라서,-g58aw의 경우 -가 짤림!!!!!

    tfidv_df = pd.DataFrame(tfvect_matrix.toarray(), columns = sorted(tfidf_col)) #tf-idf적용 df생성
    tfidv_df.index = df['id']
    #print(tfidv_df)
    ''''
                  g58aw  nt950xdz    s10      갤럭시     갤럭시북    연예인      중고폰      지갑
    id                                                                                       
    153679680  0.000000  0.000000  0.000000  0.707107  0.000000  0.000000  0.707107  0.000000
    153680960  0.000000  0.000000  0.000000  0.000000  0.000000  0.447214  0.000000  0.894427
    153682496  0.440362  0.440362  0.440362  0.334907  0.440362  0.000000  0.334907  0.000000
    '''
    table = pa.Table.from_pandas(tfidv_df)
    pq.write_table(table, 'C:\\Users\\gg664\\IdeaProjects\\Bigdata_Study\\NewSearch\\mysite\\search\\data\\test_tfidf.parquet')

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