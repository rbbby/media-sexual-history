import kblab
import os, json
import numpy as np
import pandas as pd
import multiprocessing
import re
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm

def get_json(package_id, file):
	raw = requests.get(f"https://datalab.kb.se/{package_id}/{file}.json", auth=HTTPBasicAuth("demo", credentials))	
	if raw.status_code != 200:
		print(f'{file} error in {package_id}')
		return None
	return json.loads(raw.text)

def text_scraper(package_id):
	meta_json = get_json(package_id, 'meta')
	if not meta_json:
		return ['']*3

	date = meta_json.get('created', '')
	year = int(meta_json.get('year', ''))
	if int(year) < 1900:
		return ['']*3

	content_json = get_json(package_id, 'content')
	if not content_json:
		return ['']*3

	data = []
	for box in content_json:
		s = box.get('content', '')
		s = s.lower()
		url = box.get('@id', '')
		for _, w, exp in patterns.itertuples():
			if match := re.findall(exp, s):
				data.append([s, date, url])
				break # only need 1 match
	return data

# This will not run / be loaded within the subprocess
def main():
	a = kblab.Archive('https://datalab.kb.se', auth=('demo', credentials))
	issues = {'label': 'DAGENS NYHETER'}
	max_issues = None
	checkpoint = 50
	data = []
	
	with multiprocessing.Pool() as pool:
		protocols = a.search(issues, max=max_issues)
		for i, d in enumerate(tqdm(pool.imap(text_scraper, protocols), total=protocols.n)):
			for j in d:
				j.append(i)
			
			data.extend(d)
			if i % checkpoint == 0 and i != 0:
				df = pd.DataFrame(data, columns=['text', 'date', 'url', 'index'])
				df = df.loc[df["text"] != '']
				df.to_csv('test.csv', index=False, sep='\t')
				print(f'Checkpoint made at i={i}')

patterns = pd.read_csv('data/patterns.csv')
with open(os.path.expanduser('~/keys/kb-credentials.txt'), 'r') as f:
	credentials = f.read().strip('\n')

if __name__ == '__main__':
	main()