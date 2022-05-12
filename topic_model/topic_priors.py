'''
Topic priors from csv to Mallet format 
TODO: use spacy(?) to add all inflexions
'''
import pandas as pd

filename = "topic_model/data/topic_priors_1.csv"
df = pd.read_csv(filename)

f = open(filename.replace('csv', 'txt'), 'w')
for i, topic in enumerate(sorted(set(df['topic']))):
	line = ', '.join(df.loc[df['topic'] == topic, 'word'].tolist())
	f.write(f'{i}, ' + line + '\n')

