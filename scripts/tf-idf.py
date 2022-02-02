'''
TODO: Figure out why it does not weigh down common words.
'''
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.stem import SnowballStemmer

df = pd.read_csv('test1.csv', sep='\t')
df.dropna(inplace=True)
df = df.reset_index(drop=True)

# Basic cleaning
df["text"] = df["text"].str.replace('-', '', regex=False)
df["text"] = df["text"].str.replace(r'[^A-Za-zÀ-ÿ\s]', '', regex=True)
df["text"] = df["text"].apply(lambda t: ' '.join(t.split()))
df["year"] = df["date"].str[:4].astype(int)

# Stemming
stemmer = SnowballStemmer("swedish")
df["text"] = df.text.str.split().apply(lambda x: ' ' .join([stemmer.stem(t) for t in x]))

# TF-IDF
vectorizer = TfidfVectorizer(max_df=0.9, min_df=5)
tfidf = df.groupby("year")["text"].apply(lambda x: ' '.join(x))
idx = tfidf.index
tfidf = vectorizer.fit_transform(tfidf) # (doc, w), value
vocab = vectorizer.get_feature_names_out()
dfidf = pd.DataFrame(data=tfidf.toarray(), index=idx, columns=vocab)

results = []
for _, row in dfidf.iterrows(): 
	results.append(row.sort_values(ascending=False)[:30].index)
results = pd.DataFrame(np.array(results).T, columns=dfidf.index)
print(results)