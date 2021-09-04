import nltk
from nltk import sent_tokenize
from nltk import word_tokenize
nltk.download('punkt')

def tokenize(text):
    sentences = sent_tokenize(text) #문장별로 분리
    word_tokens = [word_tokenize(sent) for sent in sentences] #분리된 문장에서 단어별로 분리
    return word_tokens

text_sample = "The Matrix is everywhere its all around us, here even in this room.\
            You can see it out your window or on your television. \
            You feel it whene you go to work, or go to church or pay your taxes."

word_tokens = tokenize(text_sample)
print(type(word_tokens), len(word_tokens))
print(word_tokens)

#참고로, python script 파일명을 token으로 지정할경우 import 패키지와 이름이 같아서 문제발생 
