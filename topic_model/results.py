import os
import numpy as np
import pandas as pd
from plotnine import ggplot, aes, geom_line, geom_abline, ggtitle, scale_x_discrete, scale_y_discrete
import matplotlib.pyplot as plt
import re
from tqdm import tqdm

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

run = 'RunSuite2022-05-09--16_44_18'
run_path = f"../PartiallyCollapsedLDA/Runs/{run}/{run.replace('Suite', '')}"

cfg = get_config(run_path)
