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
		url = block['@id']
		part, *_ = url.split('#')[-1].split('-')
		if block['@type'] == 'Text' and part == '1' and len(text.split()) > 20:
			data.append([text, date, url])

	return pd.DataFrame(data, columns=['text', 'date', 'url'])

# This will not run / be loaded within the subprocess
def main():
	'''
	Parallellized counting of word frequencies 
	'''
	files = list(set([f.split('_')[0] for f in os.listdir(path_data)]))
	data = []
	with multiprocessing.Pool() as pool:
		i = 0
		for df in tqdm(pool.imap(text_scraper, files), total=len(files)):
			data.append(df)
			i += 1
			if i % 10000 == 0:
				df = pd.concat(data)
				df.reset_index(drop=True).to_feather(os.path.join(path, 'feathers', f'dn_part1_split{str(i)[0]}.feather'))
				data = []
		
		# Last one
		df = pd.concat(data)
		df.reset_index(drop=True).to_feather(os.path.join(path, 'feathers', f'dn_part1_split{str(i+1)[0]}.feather'))

				
# Globals
patterns = pd.read_csv(f'data/patterns.csv')
path = '/media/robin/data/dn-1900-present'
path_data = os.path.join(path, 'jsonfiles')

if __name__ == '__main__':
	main()