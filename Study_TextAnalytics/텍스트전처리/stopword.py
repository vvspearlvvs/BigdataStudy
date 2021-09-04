import nltk
from nltk import word_tokenize

nltk.download('stopwords')

#stopwords 살펴보기
eng_stop = nltk.corpus.stopwords.words('english')
print("stopwords(eng) len: ", len(eng_stop))
print(eng_stop[:20])

#한국어 stopwords 지정하기ㅅ
example = "하 그런데 NTLK는 한국어 불용어 데이터를 제공하지 않습니다. \
           그냥 토큰화 단계에서 필요없는 조사, 접속사를 제거하면 되기 때문입니다."
stop_words = "하 그런데 그냥 되기 때문입니다"

word_tokens = word_tokenize(example)
stop_words=stop_words.split(' ')

result = []
for w in word_tokens:
    if w not in stop_words:
        result.append(w) # 불용어가 아닌 단어들만 사용한다
#stopword제거된상태        
print(result)
