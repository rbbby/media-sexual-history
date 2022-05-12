import os
import numpy as np
import pandas as pd
from plotnine import ggplot, aes, geom_line, geom_abline, ggtitle, scale_x_discrete, scale_y_discrete
import matplotlib.pyplot as plt
import re
from tqdm import tqdm
import polars as pl
import random

from torch.utils.data import DataLoader
from torch.utils.data import IterableDataset

class MalletDataset(IterableDataset):

    def __init__(self, filename):
        self.filename = filename

    def __len__(self):
        return sum(1 for line in open(self.filename))

    def line_mapper(self, line):
        return line

    def __iter__(self):
        file_itr = open(self.filename)
        mapped_itr = map(self.line_mapper, file_itr)
        return mapped_itr

def get_config(run_path):
	# Find config
	f = open(os.path.join(run_path, "SpaliasConvergence.txt"))
	for line in f:
		if match := re.search(r'(?:--run_config=)(.*)', line):
			cfg_name = os.path.split(match.group(0))[-1]
			break

	# Store contents in dictionary
	cfg = {}
	store_data = False
	f = open(os.path.join(run_path, "Spalias", cfg_name))
	for line in f:
		line = line.replace('\n', '')
		if store_data:
			line = line.replace('=', '')
			key, *value = line.split()
			cfg[key] = ' '.join(value)

		if line.startswith('[') and line.endswith(']'):
			store_data = True

	# Add more relevant statistics from console output
	f = open(os.path.join(run_path, "Spalias/Spalias_console_output.txt"))
	for line in f:
		if match := re.search(r'(?:Instance list is: )(.*)', line):
			cfg['M'] = match.group(1)
			break
	return cfg

def plot_convergence(run_path):
	f = open(os.path.join(run_path, 'likelihood.txt'))
	df = pd.DataFrame([l.split() for l in f.readlines()], columns=['Epoch', 'Likelihood'])
	df = df.astype(dtype={'Epoch':int, 'Likelihood':float})
	p = (
		ggplot(data=df)
		+ aes(x="Epoch", y='Likelihood')
		+ geom_line()
		+ ggtitle(f"Convergence of {os.path.split(run_path)[-1]}")
		)
	return p

def table_words(run_path, cfg, top_words=True):
	table = 'Top' if top_words else 'Relevance'
	df = pd.read_csv(cfg['topic_prior_filename'].replace('txt', 'csv'))
	df = df[['theme', 'topic', 'topic_id']].drop_duplicates()
	words = pd.read_csv(os.path.join(run_path, f'Spalias/{table}Words.txt'), header=None)
	data, topic_ids = [], []
	for _, row in df.iterrows():
		topic_ids.append(i := row['topic_id'])
		data.append(row[['theme', 'topic']].tolist()+words.loc[i].tolist())
	df = pd.DataFrame(np.array(data).T, index=['theme', 'topic']+list(range(words.shape[1])), columns=topic_ids)
	return df

def document_topic_matrix(run_path, cfg, burn_in):
	Nd = np.zeros((int(cfg['M']), int(cfg['topics'])), dtype=int)
	
	# Find z files
	files = []
	for file in os.listdir(os.path.join(run_path, 'Spalias')):
		if z := re.search(r'(?:z_)(\d+)', file):
			if int(z.group(1)) >= burn_in:
				files.append(file)
	
	print("Computing document_topic_matrix over the whole posterior")
	for file in sorted(files):
		print(f"File {file} started.")
		f = open(os.path.join(run_path, 'Spalias', file))
		for i, line in tqdm(enumerate(f), total=int(cfg['M'])):
			line = line.replace('\n', '').split(',')
			line = list(map(int, line))
			np.add.at(Nd[i], line, 1)
		f.close()

def load_phi(run_path, cfg):
	df = pd.read_csv(cfg['topic_prior_filename'].replace('txt', 'csv'))
	d = {row['topic']:row['topic_id'] for _, row in df.iterrows()}
	userows = list(set(df['topic_id']))
	phi = pd.read_csv(	os.path.join(run_path, 'phi-mean.csv'),
						header=None, skiprows=lambda x: x not in d.values())
	phi.index = list(d.keys())
	return phi


runs = {'small':'RunSuite2022-05-09--16_44_18', 'big':'RunSuite2022-05-10--14_33_46'}
run = runs['big']

run_path = f"../PartiallyCollapsedLDA/Runs/{run}/{run.replace('Suite', '')}"

cfg = get_config(run_path)

#tab = table_words(run_path, cfg, top_words=True)



#df = pl.read_csv(os.path.join(run_path, 'doc_topic_theta.csv'))
#df = df.to_pandas()
#df = df.loc[:10,:10]
#df.to_csv('test.csv', index=False)

#print(time()-start)
#
#df = None

def get_top_doc_ids(run_path, cfg, n=10):
	df = pd.read_csv(cfg['topic_prior_filename'].replace('txt', 'csv'))
	d = {row['topic']:row['topic_id'] for _, row in df[['topic', 'topic_id']].drop_duplicates().iterrows()}
	theta = pd.read_csv(os.path.join(run_path, 'doc_topic_theta.csv'), header=None, names=d.keys(), usecols=d.values())
	data = []
	for topic in d.keys():
		data.append(list(theta[topic].nlargest(n).index))
	df = pd.DataFrame(list(map(list, zip(*data))), columns=d.keys())
	
	return df


def get_top_doc_string(run_path, cfg, n=10):
	df = get_top_doc_ids(run_path, cfg, n)
	dataset = MalletDataset(cfg['dataset'])
	dataloader = DataLoader(dataset, batch_size = 1)
	data = []
	for i, lines in enumerate(tqdm(dataloader, total=len(dataloader))):
		x, y = np.where(df == i)
		if x.size > 0:
			topic = df.iloc[:,y[0]].name
			data.append([y[0], topic]+lines[0].split('\t'))
	df = pd.DataFrame(data, columns=['topic_id', 'topic', 'dark_id', 'date', 'text'])
	df = df.sort_values('topic_id')

	s = ''
	for _, row in df.iterrows():
		text = row['text'].split()
		text = ' '.join(sorted(text))
		s += f"topic_id: {row['topic_id']}, topic: {row['topic']}, dark_id: {row['dark_id']}, date: {row['date']}\n{text}\n\n"
	return s








