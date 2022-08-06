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
from operator import itemgetter


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
	print(root)
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


def get_phi(root, chunksize):
	data = []
	for chunk in pd.read_csv(
		os.path.join(root, 'phi-means.csv'),
		header=None, dtype=float, chunksize=chunksize):
		data.append(chunk)

	vocab = get_vocab(root)
	df = pd.concat(data, ignore_index=True)
	df.columns = vocab.keys()
	return df


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


def table_words(root, cfg, phi, n=20, top=True, relevance=True):
	seed_words = get_seed_words(root, cfg)
	seed_topics = list(sorted(set(seed_words['topic_id'])))
	tables = []
	if top:
		d = {}
		for i, row in phi.loc[seed_topics].iterrows():
			d[i] = list(row.nlargest(n).index)
		tables.append(pd.DataFrame(d))

	if relevance:
		phi = phi.div(phi.sum(axis=0), axis=1)
		for i, row in phi.loc[seed_topics].iterrows():
			d[i] = list(row.nlargest(n).index)
		tables.append(pd.DataFrame(d))
	
	return tables


def no_co_location_line(line, target_topic_indices):
	text = line
	text = text.rstrip('\n')
	text = text.split(',')
	text = list(map(int, text))
	for i in target_topic_indices:
		print(i)
		if i in text:
			return text


def learnt_words_for_single_topic(root, k, phi, seed_words, n):
	phi = phi.loc[k]
	target_seed_words = seed_words.loc[seed_words['topic_id'] == k, 'word'].tolist()
	phi = phi.drop(target_seed_words, errors='ignore')
	with open(os.path.join(root, 'default/corpus.txt'), 'r') as f:
		for line in f:
			text = line
			text = text.rstrip('\n')
			text = text.split('\t')[-1]
			text = text.split()
			if any(w for w in text if w in target_seed_words):
				phi = phi.drop(text, errors='ignore')

	phi = phi.sort_values(ascending=False)[:n]
	return list(phi.index)


def learnt_words(root, cfg, phi, n=5):
	'''
	Top n words with:
		1. highest prob to occur in seeded topics
		2. no cooccurences with target topic seeded words.

	'''	
	seed_words = get_seed_words(root, cfg)
	seed_topics = list(sorted(set(seed_words['topic_id'])))

	# We wont need the other rows
	phi = phi.loc[seed_topics]

	data = []
	for k in seed_topics:
		learnt_words_k = learnt_words_for_single_topic(root, k, phi, seed_words, n)
		data.append(learnt_words_k)

	# Transpose list of lists
	data = list(map(list, zip(*data)))
	df = pd.DataFrame(data, columns=seed_topics)
	return df


def get_z_filepaths(root, cfg):
	p = Path(root)
	z_files = list(p.glob('default/z_[0-9]*.csv'))
	iterations = int(cfg.get('iterations'))
	percent_burn_in = int(cfg.get('phi_mean_burnin', 0)) / 100
	burn_in = iterations * percent_burn_in
	pattern = r'(?:z_)([0-9]+)(?:.csv)'
	z_files = [f for f in z_files if burn_in <= int(re.search(pattern, str(f)).group(1))]
	return z_files


def get_seed_words(root, cfg):
	with open(cfg.get('topic_prior_filename'), 'r') as f:
		data = []
		for line in f:
			topic_id, *words = line.rstrip('\n').split(', ')
			for word in words:
				data.append([int(topic_id), word])
		df = pd.DataFrame(data, columns=['topic_id', 'word'])
		# Drop all duplicated rows to get rid of anti topics
		df = df.drop_duplicates(keep=False)
		df = df.sort_values('topic_id')
	return df


def get_metadata(cfg):
	M = int(cfg.get('M'))
	dates = []
	with open(cfg['dataset'], 'r') as f:
		for line in tqdm(f, total=M):
			dark_id, date, *_ = line.split('\t')
			year, month, day = date.split('-')
			dates.append(year) # only use year for now
	return dark_id, dates


def compute_document_topic_matrix(root, cfg):
		M, K = list(map(int, itemgetter('M', 'topics')(cfg)))
		z_files = get_z_filepaths(root, cfg)
		Nd = np.zeros((M, K), dtype=float)
		
		for z_file in z_files:
			with open(z_file, 'r') as f:	
				for i, line in tqdm(enumerate(f), total=len(Nd)):
					if not line.isspace():
						topic_indicators = list(map(int, line.split(',')))
						np.add.at(Nd[i], topic_indicators, 1)

		# Take sample average over runs
		return Nd / len(z_files)


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