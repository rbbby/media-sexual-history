import os, json
import pandas as pd
import random
import multiprocessing
from tqdm import tqdm

def box_scraper(package_id):
	with open(os.path.join(path_data, package_id+'_meta.json'), 'r') as f:
		meta = json.load(f)
	with open(os.path.join(path_data, package_id+'_content.json'), 'r') as f:
		content = json.load(f)
	date = meta['created']
	data = []
	for c in content:
		if c['@type'] == 'Text':
			data.append([date, c['@id']])
	return data

def main():
	files = list(set([f.split('_')[0] for f in os.listdir(path_data)]))
	data = []
	with multiprocessing.Pool() as pool:
		for d in tqdm(pool.imap(box_scraper, files), total=len(files)):
			data.extend(d)
	df = pd.DataFrame(data, columns=['date', 'box'])
	df.to_csv(os.path.join(path, 'boxes.csv'), index=False)

path = '/media/robin/data/dn-1900-present'
path_data = os.path.join(path, 'jsonfiles')

if __name__ == '__main__':
	main()