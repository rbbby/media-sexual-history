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
from src.dataset import MediaDataset
from src.evaluation import predict
from src.models import BertMetaClassifier
import transformers
from datetime import date
import random


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
			# Select part 1
			if not res['@id'].split('#')[-1].split('-')[0] == str(1):
				continue
			if res['content']:
				text = res['content'][0]
				if len(text.split()) >= threshold:
					idx = res['@id'].split('/')[-1]
					data.append([idx, date, year, weekday, text])

	df = pd.DataFrame(data, columns=['idx', 'date', 'year', 'weekday', 'content'])
	
	# Sample duplicate papers
	df['dark_id'] = df['idx'].apply(lambda x: x.split('#')[0])
	dates = set(df['date'])
	for date in dates:
	    dark_ids = set(df.loc[df['date'] == date, 'dark_id'])
	    if len(dark_ids) > 1:
	    	duplicates = [d for d in dark_ids if d not in random.sample(dark_ids, 1)]
	    	df = df[~df['dark_id'].isin(duplicates)].reset_index(drop=True)
	    	
	return df

def predict_df(df, model, device):
	df['tag'] = np.ones(len(df))
	dataset_test = MediaDataset(df=df)
	testloader = torch.utils.data.DataLoader(
	    dataset_test, batch_size=16, shuffle=False, num_workers=4)
	df, _ = predict(df, testloader, model=model, device=device)
	df = df[df['pred'] == 1]
	return df


def main(args):
	with open(args.credentials, 'r') as file:
	    pw = file.read().replace('\n', '')
	a = Archive("https://datalab.kb.se", auth=("demo", pw))

	model = BertMetaClassifier()
	model.load_state_dict(torch.load(args.model_filename)['model_state_dict'])
	model.cuda()

	try:
		os.remove(args.outfile)
	except:
		pass

	for year in range(2000, 2022):
		print(f"Year {year} started.")
		package_ids = a.search({"label": "DAGENS NYHETER", "meta.created": year})
		with multiprocessing.Pool() as pool:
			data = []
			for df in tqdm(pool.imap(get_data, package_ids), total=package_ids.n):
				data.append(df)
		df = pd.concat(data).reset_index(drop=True)
		df = predict_df(df, model, device=args.device)

		# Mallet format dataset
		df = df[['idx', 'date', 'text']]
		df.to_csv(args.outfile, header=None, index=None, sep='\t', mode='a')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model_filename", type=str) # path/to/model.pth
    parser.add_argument("--outfile", default="corpus.txt", type=str)
    parser.add_argument("--device", default="cuda", type=str)
    parser.add_argument("--credentials", type=str) # path/to/credentials.txt
    args = parser.parse_args()
    main(args)
