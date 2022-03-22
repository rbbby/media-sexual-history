import kblab
import os, json
import numpy as np
import pandas as pd
import multiprocessing
import re
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
import time

def store_json(package_id):
	try:
		meta = requests.get(f"https://datalab.kb.se/{package_id}/meta.json", auth=HTTPBasicAuth("demo", credentials))
		meta = json.loads(meta.text)
		content = requests.get(f"https://datalab.kb.se/{package_id}/content.json", auth=HTTPBasicAuth("demo", credentials))	
		content = json.loads(content.text)

		with open(os.path.join(path_data, package_id+'_meta.json'), 'w') as f:
			json.dump(meta, f, indent=2, ensure_ascii=False)

		with open(os.path.join(path_data, package_id+'_content.json'), 'w') as f:
			json.dump(content, f, indent=2, ensure_ascii=False)

	except:
		return package_id
	
def get_json(package_id, file):
	raw = requests.get(f"https://datalab.kb.se/{package_id}/{file}.json", auth=HTTPBasicAuth("demo", credentials))	
	if raw.status_code != 200:
		print(f'{file} error in {package_id}')
		return None
	return json.loads(raw.text)

def text_scraper(package_id):
	try:
		meta_json = get_json(package_id, 'meta')
	except:
		return ['']*3	

	date = meta_json.get('created', '')
	year = int(meta_json.get('year', ''))
	if int(year) < 1900:
		return ['']*3

	try:
		content_json = get_json(package_id, 'content')
	except:
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
	start = time.time()
	a = kblab.Archive('https://datalab.kb.se', auth=('demo', credentials))
	issues = {'label': 'DAGENS NYHETER'}
	max_issues = None
	data = []

	log = open(os.path.expanduser('../home/robin/repos/media-sexual-history/log.txt'), 'w')

	with multiprocessing.Pool() as pool:
		protocols = a.search(issues, max=max_issues)
		print(f'Total number of protocols: {protocols.n}')
		for i, d in enumerate(pool.imap(store_json, protocols)):
			if d:
				log.write(d)
				log.write('\n')
			if i % 500 == 0 and i != 0:
				print(f'Protocol {i} finished after {time.time()-start}')

# Input
patterns = pd.read_csv(f'data/patterns.csv')
with open(os.path.expanduser('../../keys/kb-credentials.txt'), 'r') as f:
	credentials = f.read().strip('\n')

# Output
root = os.path.expanduser('~')
os.chdir(root)

path_data = '../../media/r3/data/dn_1900_present'

if __name__ == '__main__':
	main()