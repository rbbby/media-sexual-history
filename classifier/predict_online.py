import numpy as np
import pandas as pd
from tqdm import tqdm
import multiprocessing
from datetime import datetime
from kblab import Archive
from kblab.utils import flerge
import os
import torch
import torch.nn as nn
import numpy as np
from src.dataset import *
from src.evaluation import *
from src.models import *
import transformers

def get_data(package_id, threshold=20):
	data = []
	package = a.get(package_id)
	result = flerge(package)

	if result:
		date = datetime.fromisoformat(result[0]['meta']['created'])
		weekday = date.weekday()
		year = date.year
		date = date.strftime("%Y-%m-%d")
		for res in result:
			if not res['@id'].split('#')[-1].split('-')[0] == str(1):
				continue
			if res['content']:
				text = res['content'][0]
				if len(text.split()) >= threshold:
					idx = res['@id'].split('/')[-1]
					data.append([idx, date, year, weekday, text])

	return pd.DataFrame(data, columns=['idx', 'date', 'year', 'weekday', 'text'])

def predict_df(df, model):
	df['label'] = np.ones(len(df))
	dataset_test = MediaDataset(df=df)
	testloader = torch.utils.data.DataLoader(
	    dataset_test, batch_size=16, shuffle=False, num_workers=4)
	df, _ = predict(df, testloader, model=model)
	df = df[df['pred'] == 1]
	return df


with open('../../keys/kb-credentials.txt', 'r') as file:
    pw = file.read().replace('\n', '')
a = Archive("https://datalab.kb.se", auth=("demo", pw))

outfile = "/media/robin/dn/dn.txt"
n_words = 20
model = BertMetaClassifier()
model.load_state_dict(torch.load(f'models/best_model_{n_words}.pth')['model_state_dict'])
model.cuda()

try:
	os.remove(outfile)
except:
	pass

for year in range(1900, 2022):
	print(f"Year {year} started.")
	package_ids = a.search({"label": "DAGENS NYHETER", "meta.created": year})
	with multiprocessing.Pool() as pool:
		data = []
		for df in tqdm(pool.imap(get_data, package_ids), total=package_ids.n):
			data.append(df)
	df = pd.concat(data).reset_index(drop=True)
	df = predict_df(df, model)

	# Mallet format dataset
	df = df[['idx', 'date', 'text']]
	df.to_csv(outfile, header=None, index=None, sep='\t', mode='a')
