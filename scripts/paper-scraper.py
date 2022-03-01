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

def text_scraper(package_id):
	data = []
	with open(os.path.join(path_data, package_id+'_meta.json'), 'r') as f:
		meta = json.load(f)
	with open(os.path.join(path_data, package_id+'_content.json'), 'r') as f:
		content = json.load(f)

	date = meta['created']

	for block in content:
		text = block['content']
		for _, w, exp in patterns.itertuples():
			if match := re.findall(exp, text):
				url = block['@id']
				part, page, *_ = url.split('#')[-1].split('-')
				data.append([text, part, page, date, url, match])

	df = pd.DataFrame(data, columns=['text', 'part', 'page', 'date', 'url', 'match'])
	return df

# This will not run / be loaded within the subprocess
def main():
	'''
	Parallellized counting of word frequencies 
	'''
	files = sorted(list(set([f.split('_')[0] for f in os.listdir(path_data)])))
	data = []
	with multiprocessing.Pool() as pool:
		for df in tqdm(pool.imap(text_scraper, files), total=len(files)):
			data.append(df)
	df = pd.concat(data)
	df.to_csv('results/dn.csv', index=False)

# Globals
patterns = pd.read_csv(f'data/patterns.csv')
path_data = '/media/robin/data/newspaper-data'

if __name__ == '__main__':
	main()