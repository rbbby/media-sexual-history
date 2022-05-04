import pandas as pd
import re
import os
from jiwer import cer, wer
import matplotlib.pyplot as plt

def clean_string(s):
	s = s.replace('-', '') # Join separated words to avoid discrepancies
	s = re.sub(r'[^A-ZÀ-Þa-zß-ÿ\s\.]', '', s)
	s = ' '.join(s.split())
	s = s.lower()
	return s

data_path = '/media/robin/dn/ocr-quality/ocr-annotated-fixed.csv'

df = pd.read_csv(data_path)
df = df.fillna('')

# Use 5-year periods instead
def year_to_period(year):
	if int(str(year)[-1]) >= 5:
		return year // 10 * 10 + 5
	else:
		return year // 10 * 10

df['period'] = df['year'].apply(year_to_period)

errors = ["https://datalab.kb.se/dark-4204373#1-10-ARTICLE11157841-ZONE120095716", "https://datalab.kb.se/dark-4204392#1-6-ARTICLE14490833-ZONE162431456"]

for i, row in df.iterrows():
	if row['url'] in errors:
		df = df.drop(i, axis=0)
df = df.reset_index(drop=True)

# Clean text
df['gold'] = df['gold'].apply(clean_string)
df['ocr'] = df['ocr'].apply(clean_string)
df = df.loc[(df['gold'] != '') & (df['ocr'] != '')].reset_index(drop=True)

#for _, row in df[df['period'] == 1920].iterrows():
#	n_words = len(row['gold'].split())
#	n_sentences = len(row['gold'].split('.'))
#	print(row['gold'])
#	print(row['ocr'])
#	print(f"n_words:{n_words}, n_sentences:{n_sentences}")
#	print(row['url']+'\n')

period = sorted(list(set(df['period'])))
data = []
for p in period:
	dfp = df[df['period'] == p]
	gold = dfp['gold'].tolist()
	ocr = dfp['ocr'].tolist()
	c = round(cer(gold, ocr), 3)
	w = round(wer(gold, ocr), 3)
	n = len(dfp)
	data.append([p, c, w, n])

df = pd.DataFrame(data, columns=['period', 'cer', 'wer', 'n'])
print(df)
df.to_csv('results/ocr-quality.csv', index=False)

plt.plot(df['period'], df['cer'])
plt.plot(df['period'], df['wer'])
plt.savefig('results/ocr-quality.png')