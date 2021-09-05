import nltk 
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
corpus = [
    'This is the first document.',
    'This is the second second document.',
    'And the third one.',
    'Is this the first document?',
    'The last document?',
]
vector = TfidfVectorizer()
#print(vector.fit_transform(corpus).toarray()) # TF-IDF 값을 기록한다.
#print(vector.vocabulary_)

## CountVectorizer -> DataFrame
countvect_df = pd.DataFrame(vector.fit_transform(corpus).toarray(), columns = sorted(vector.vocabulary_))
#print(countvect_df)
