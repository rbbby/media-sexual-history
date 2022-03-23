import numpy as np
import pandas as pd
import os, json
import random

# Sample
def sample(df, n_pages_per_period):
	periods = sorted(set(df['period']))
	data = []
	for p in periods:
		dfp = df[df['period'] == p].reset_index(drop=True)
		incl_prob = 1/len(dfp)
		dfp = dfp.loc[np.random.choice(len(dfp), n_pages_per_period)]
		for _, row in dfp.iterrows():
			package_id, part, page = row[['package_id', 'part', 'page']]
			url = f"https://datalab.kb.se/{package_id}#{part}-{page}"
			data.append([url, row['date'], incl_prob, p])
	print(f'{len(data)} pages sampled.')
	return pd.DataFrame(data, columns=['url', 'date', 'prob', 'period']).sort_values('date')

def subsample(df, n_pages_per_period):
	periods = sorted(set(df['period']))
	data = []
	for p in periods:
		dfp = df[df['period'] == p].reset_index(drop=True)
		dfp = dfp.loc[np.random.choice(len(dfp), n_pages_per_period)]
		data.append(dfp)
	print(f'{len(periods)*n_pages_per_period} textblocks sampled.')
	return pd.concat(data)

# Load data
path = '/media/robin/data/dn-1900-present'
path_data = os.path.join(path, 'jsonfiles')
df = pd.read_csv(os.path.join(path, 'dn-meta.csv'))
df['year'] = df['date'].apply(lambda x: int(x[:4]))

# Filter 1900s
df = df[df['year'] >= 1900]

# Filter parts
parts = [p+1 for p in list(range(5))]
df = df[df.part.isin(parts)]

# Create 5-year periods
df['period'] = df['year'].apply(lambda x: int(str(x)[:3] + str((int(str(x)[-1]) >= 5) * 5)))

# Draw sample
np.random.seed(123)
df = sample(df, 20)
df.to_csv('data/sample.csv', index=False)

# Also sample textblocks
df = subsample(df, 4)
df['text'] = pd.Series(str)

for i, row in df.iterrows():
	package_id, part_page = row['url'].split('/')[-1].split('#')
	part, page = part_page.split('-')
	with open(os.path.join(path_data, package_id+'_content.json'), 'r') as f:
		content = json.load(f)

	data = []
	for c in content:
		url = c['@id']
		url = url.split('#')[-1]
		url = url.split('-ARTICLE')[0]
		cpart, cpage = url.split('-')
		if cpart == part and cpage == page and c['@type'] == 'Text':
			data.append([c['@id'], c['content']])
	
	textblock_url, text = data[random.sample(range(len(data)), 1)[0]]
	prob = row['prob'] * 1/len(data)
	df.loc[i, ['url', 'text', 'prob']] = textblock_url, text, prob
df.to_csv('data/ocr-quality-sample.csv', index=False)