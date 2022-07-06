import numpy as np
import pandas as pd
#import datatable as dt
import matplotlib.pyplot as plt
import os
import re
from nltk.stem import SnowballStemmer
from pathlib import Path
import torch
from torch.utils.data import IterableDataset
from plotnine import ggplot, aes, geom_line, geom_abline, ggtitle, scale_x_discrete, scale_y_discrete, theme
from functools import partial
import multiprocessing
from tqdm import tqdm

class PosteriorDataset(IterableDataset):
	'''
	Pytorch iterable dataset for postprocessing Mallet results.
	Iterates over z_files and processed corpus jointly in parallel.
	'''
	def __init__(self, root, cfg):
		p = Path(root)
		
		# Topic indicator filepaths after burn_in period
		# NOTE: only uses last draw atm
		z_files = list(p.glob('default/z_[0-9]*.csv'))
		iterations = int(cfg.get('iterations'))
		percent_burn_in = int(cfg.get('phi_mean_burnin', 0)) / 100
		burn_in = iterations * percent_burn_in
		pattern = r'(?:z_)([0-9]+)(?:.csv)'
		z_files = [f for f in z_files if burn_in <= int(re.search(pattern, str(f)).group(1))]
		self.topics = z_files[:2] # TESTING

	def process_line(self, row):
		topics = row
		topics = topics.replace('\n', '')
		topics = topics.split(',')
		topics = list(map(int, topics))
		return topics

	def line_mapper(self, topic):
		topics = list(map(self.process_line, topic))
		return topics

	def __iter__(self):
		topic_itr = (open(f) for f in self.topics)
		mapped_itr = map(self.line_mapper, topic_itr)
		for i in mapped_itr:
			print(i)
		return mapped_itr


def get_config(root):
	# Use convergence file to find config file
	f = open(os.path.join(root, "defaultConvergence.txt"))
	for line in f:
		if match := re.search(r'(?:--run_config=)(.*)', line):
			cfg_name = os.path.split(match.group(0))[-1]
			break

	# Store contents in dictionary
	cfg = {}
	store_data = False
	f = open(os.path.join(root, "default", cfg_name))
	for line in f:
		if '=' not in line:
			continue
		line = line.replace('\n', '').replace('=', '')
		key, *value = line.split()
		cfg[key] = ' '.join(value)

	# Add more relevant statistics from console output
	f = open(os.path.join(root, "default/default_console_output.txt"))
	for line in f:
		if match := re.search(r'(?:Instance list is: )(.*)', line):
			cfg['M'] = match.group(1)
			break
	return cfg


def get_vocab(root):
	with open(os.path.join(root, 'phi_vocabulary.txt'), 'r') as f:
		vocab = {line.replace('\n', ''):i for i, line in enumerate(f.readlines())}
	return vocab


def get_phi(root, datatable=False):
	if datatable:
		phi = dt.fread(os.path.join(root, 'phi-means.csv'))
		phi = phi.to_pandas()
	else:
		phi = pd.read_csv(os.path.join(root, 'phi-means.csv'))	
	vocab = get_vocab(root)
	phi.columns = vocab
	return phi


def plot_convergence(root):
	f = open(os.path.join(root, 'likelihood.txt'))
	df = pd.DataFrame([l.split() for l in f.readlines()], columns=['Epoch', 'Likelihood'])
	df = df.astype(dtype={'Epoch':int, 'Likelihood':float})
	p = (
		ggplot(data=df)
		+ aes(x="Epoch", y='Likelihood')
		+ geom_line()
		+ ggtitle(f"Convergence of {os.path.split(root)[-1]}")
#		+ theme(figure_size=(16, 8))  # here you define the plot size
		)
	return p


def table_words(root, cfg, n=20, top_words=True):
	seed_words = get_seed_words(root, cfg)
	seed_topics = list(map(int, seed_words.keys()))

	phi = get_phi(root)
	if not top_words:
		phi = phi.div(phi.sum(axis=0), axis=1)
	phi = phi.loc[seed_topics]

	d = {}
	for i, row in phi.iterrows():
		d[i] = list(row.nlargest(n).index)
	return pd.DataFrame(d)


def no_co_location_line(line, target_topic_indices):
	text = line
	text = text.replace('\n', '')
	text = text.split(',')
	text = list(map(int, text))
	for i in target_topic_indices:
		if i in text:
			return text


def learnt_words(root, cfg, target_topic, n=50):
	'''
	Top n words with:
		1. highest prob to occur in target topic
		2. no cooccurences with target topic seeded words.

	'''
	seed_words = get_seed_words(root, cfg)
	vocab = get_vocab(root)
	target_topic_words = seed_words.get(f'{target_topic}')	
	target_topic_indices = [value for key, value in vocab.items() if key in target_topic_words]

	phi = get_phi(root)
	phi = phi.loc[target_topic]
	phi = phi.drop(target_topic_words, errors='ignore')

	process_func = partial(no_co_location_line, target_topic_indices=target_topic_indices)

	with multiprocessing.Pool() as pool:
		with open(os.path.join(root, 'default/corpus.txt'), 'r') as f:
			for idx in tqdm(pool.imap(process_func, f), total=int(cfg['M'])):
				if idx:
					phi = phi.drop(idx, errors='ignore')

	phi = phi.sort_values(ascending=False)[:n]
	phi = pd.DataFrame({'word':phi.index, 'probability':phi}).reset_index(drop=True)
	return phi


def get_z_filepaths(root, cfg):
	p = Path(root)
	z_files = list(p.glob('default/z_[0-9]*.csv'))
	iterations = int(cfg.get('iterations'))
	percent_burn_in = int(cfg.get('phi_mean_burnin', 0)) / 100
	burn_in = iterations * percent_burn_in
	pattern = r'(?:z_)([0-9]+)(?:.csv)'
	z_files = [f for f in z_files if burn_in <= int(re.search(pattern, str(f)).group(1))]
	return z_files


def get_seed_words(cfg):
	df = pd.read_csv(cfg.get('topic_prior_filename').replace('txt', 'csv'), sep=';')
	df = df[df['topic_id'] != -1]
	df = df.drop_duplicates(subset=['topic', 'topic_id'])
	df = df.sort_values('topic_id')
	return {row['topic']:row['topic_id'] for _, row in df.iterrows()}


def get_metadata(cfg):
	M = int(cfg.get('M'))
	dates = []
	with open(cfg['dataset'], 'r') as f:
		for line in tqdm(f, total=M):
			dark_id, date, *_ = line.split('\t')
			year, month, day = date.split('-')
			dates.append(year) # only use year for now
	return dark_id, dates


def compute_theta(root, cfg):
	alpha = float(cfg.get('alpha'))
	M, K = list(map(int, itemgetter('M', 'topics')(cfg)))
	z_files = get_z_filepaths(root, cfg)
	Nd = np.zeros((M, K), dtype=float)
	Nd += alpha * len(z_files)
	for z_file in z_files:
		with open(z_file, 'r') as f:	
			for i, line in tqdm(enumerate(f), total=len(Nd)):
				if not line.isspace():
					topic_indicators = list(map(int, line.split(',')))
					np.add.at(Nd[i], topic_indicators, 1)
	theta = Nd / Nd.sum(axis=1)[:, np.newaxis]
	return theta

def plot_topic_salience(theta, time, seed_dict):
	theta = theta.groupby('time').mean()
	cc = (cycler(color=list('bgrcmyk')) *
	      cycler(linestyle=['-', '--', '-.', '-', '--', '-.', '-']))
	fig, ax = plt.subplots()
	ax.set_prop_cycle(cc)
	for key in seed_dict.keys():
		ax.plot(theta.index, theta[key], label=key)
	ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
	return fig, ax