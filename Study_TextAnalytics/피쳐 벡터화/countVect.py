import nltk 
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
corpus = [
    'This is the first document.',
    'This is the second second document.',
    'And the third one.',
    'Is this the first document?',
    'The last document?',
]
vector = CountVectorizer()
#print(vector.fit_transform(corpus).toarray()) # 코퍼스로부터 각 단어의 빈도 수를 기록한다.
#print(vector.vocabulary_)

## CountVectorizer -> DataFrame
countvect_df = pd.DataFrame(vector.fit_transform(corpus).toarray(), columns = sorted(vector.vocabulary_))
print(countvect_df)


## 1. stop_words 파라미터
# stop_words(불용어) 직접 지정 
vect = CountVectorizer(stop_words=["and", "is", "the", "this"]).fit(corpus) 
# 영어용 stop_words를 적용할 경우(한국어는 지원불가) 
vect = CountVectorizer(stop_words="english").fit(corpus)

## 2. token 파라미터
# analyzer : 문자열 {‘word’, ‘char’, ‘char_wb’} 또는 함수 사용 
vect = CountVectorizer(analyzer="char").fit(corpus) 
# token_pattern : 토큰정의용 정규표현식 
vect = CountVectorizer(token_pattern="t\w+").fit(corpus) 
# tokenizer : 직접 정의한 토큰생성 함수 또는 None (디폴트) 
vect = CountVectorizer(tokenizer=nltk.word_tokenize).fit(corpus) 

## 3. 빈도수 파라미터
# max_df, min_df : 토큰의 빈도수가 max_df,min_df를 초과하면 무시 
vect = CountVectorizer(max_df=4, min_df=2).fit(corpus)
