from django.test import TestCase
import os
import pandas as pd
import re,json,csv
from collections import defaultdict

# Create your tests here.

def data_load():
    df = pd.read_csv("data/prodct_name.tsv", sep='\t')
    print(df.tail(2))

    with open("data/prodct_name.tsv", encoding='UTF8') as raw_file:
        reader = csv.reader(raw_file,delimiter ='\t')
        next(reader)
        for row in reader:
           print(row)


def test_data():
    test_data = [[10,'갤럭시 중고폰'],[11,'연예인 지갑 중고폰'],[12,'갤럭시s10 갤럭시북 NT950XDZ-G58AW 지갑']]
    test_df = pd.DataFrame(test_data, columns=['id','name'])
    return test_df

def tokenizer(data):
    token = []

    data = data.lower() #소문자로 변환

    words = data.split() #공백으로 분리
    #print("split ", words)

    #규칙에 해당 -> findall
    p = re.compile("[가-힣]+|[ㄱ-ㅎ|ㅏ-ㅣ]+|[0-9a-z]+|[a-z0-9-]+|[^ a-z0-9가-힣+]") #규칙

    for word in words:
        find = re.findall(p,word)
        for w in find:
            token.append(w)
    return token

def inverted_index(json_data):

    index_dict=defaultdict(list)

    for data in json_data:
        for token in data['token']:
            if token in data['token']:
                if token in index_dict:
                    index_dict[token].append(data['id'])
                else:
                    index_dict[token]=[data['id']]

    return index_dict

if __name__=='__main__':
    data_load()
    '''
    test = test_data()
    test['token']=test['name'].apply(tokenizer)

    js = test.to_json(orient = 'records')
    test_json =json.loads(js)
    print(test_json)

    test_index = inverted_index(test_json)
    print(test_index)
    '''
