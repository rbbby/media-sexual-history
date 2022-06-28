import numpy as np
import pandas as pd
import os
import re
import matplotlib.pyplot as plt
from pathlib import Path
import argparse
from torch.utils.data import IterableDataset, DataLoader
from PCPLDA.results import (get_config, get_vocab, get_phi, get_seed_words,
							table_words,
							plot_convergence,
							learnt_words,
							PosteriorDataset,
							)
import torch
from torch.utils.data import DataLoader
from operator import itemgetter
from tqdm import tqdm
from PCPLDA.report import (add_plot, add_part, output_df_to_pdf, split_table, add_table, split_text)
import datatable as dt
from tqdm import tqdm

def main(args):
	
	args.root = '../PartiallyCollapsedLDA/Runs/RunSuite2022-06-15--17_18_24/Run2022-06-15--17_18_24'
	
	# Produce results
	cfg = get_config(args.root)

	def get_dates():
		data = []
		with open(cfg.get('dataset'), 'r') as f:
			for line in tqdm(f, total=int(cfg.get('iterations'))):
				date = line.split('\t')[1]
				year, month, day = date.split('-')
				data.append([year, month, day])
		return pd.DataFrame(data, columns=['year', 'month', 'day'])

	def theta_wip():
		alpha = float(cfg.get('alpha'))
		chunksize = 10000
		n_chunks = sum(1 for row in open('/media/robin/dn/PCPLDA/Nd.csv', 'r'))
		print(n_chunks)

		chunks = []
		with pd.read_csv('/media/robin/dn/PCPLDA/Nd.csv', chunksize=chunksize, dtype=float) as reader:
			for chunk in tqdm(reader, total=n_chunks):
				chunk += alpha
				chunks.append(chunk.div(chunk.sum(axis=1), axis=0))

		theta = pd.concat(chunks)
		theta.to_csv('/media/robin/dn/PCPLDA/theta.csv', index=False)




	return
	alpha = float(cfg.get('alpha'))

	def get_z_filepaths(root, cfg):
		p = Path(root)
		z_files = list(p.glob('default/z_[0-9]*.csv'))
		iterations = int(cfg.get('iterations'))
		percent_burn_in = int(cfg.get('phi_mean_burnin', 0)) / 100
		burn_in = iterations * percent_burn_in
		pattern = r'(?:z_)([0-9]+)(?:.csv)'
		z_files = [f for f in z_files if burn_in <= int(re.search(pattern, str(f)).group(1))]
		return z_files

	z_files = get_z_filepaths(args.root, cfg)

	Nd = np.zeros(list(map(int, itemgetter('M', 'topics')(cfg))), dtype=int)
	for z_file in z_files:
		with open(z_file, 'r') as f:	
			for i, line in tqdm(enumerate(f), total=len(Nd)):
				topic_indicators = list(map(int, line.split(',')))
				np.add.at(Nd[i], topic_indicators, 1)
		break

	#pd.read_parquet('df.parquet.gzip')

#	theta = (Nd+alpha) / (Nd+alpha).sum(axis=1)[:, np.newaxis]
#
#	with open('/media/robin/dn/PCPLDA/theta.npy', 'wb') as f:
#	    np.save(f, theta)

#	import fileinput
#	with fileinput.input(files=z_files) as f:
#		for line in f:
#			print(line)
#			break
#			process(line)

#	dataset = PosteriorDataset(args.root, cfg)
#	dataloader = DataLoader(dataset, batch_size=1)
	#Nd = np.zeros(list(map(int, itemgetter('M', 'topics')(cfg))), dtype=int)
#	for i, batch in tqdm(enumerate(dataloader), total=int(cfg.get('M'))):
#		print(i)
#		topics = torch.concat(batch[1]).numpy()
#		np.add.at(Nd[i], topics, 1)
#		if i == 10:
#			break

		

	return

	p = plot_convergence(args.root).draw()
	p.savefig('convergence.png', dpi=300)
	#top = table_words(args.root, cfg, top_words=True)
	#relevance = table_words(args.root, cfg, top_words=False)
	#learnt = learnt_words(args.root, cfg, args.target_topic)
	top = pd.read_csv('top.csv')
	relevance = pd.read_csv('relevance.csv')
	learnt = pd.read_csv('learnt.csv') # should not be a table
	learnt = ', '.join(learnt['word'].tolist())
	
	# Result report
	w, h = 40, 5

	# 1. Set up the PDF doc basics
	pdf = FPDF()
	pdf.add_page()
	
	### Document title
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, f"PCLDA result report {os.path.split(args.root)[-1]}", ln=1)
	pdf.ln()
	add_part(pdf, 1, w, h)

	### Config
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, 'Config', ln=1)
	pdf.set_font('Times', size=12)
	for key, value in cfg.items():
		pdf.cell(w, h, f"{key}: {value}", ln=1)
	pdf.ln()

	### Convergence
	title = 'Convergence'
	description = 'The evaluation function should reach some stable value that it jumps around randomly.'
	plot = 'convergence.png'
	add_plot(pdf, plot, title, description, w, h)

	### Part 2
	pdf.add_page(orientation='L')
	add_part(pdf, 2, w, h)

	### Top words
	for i, table in enumerate(split_table(top, axis=1)):
		if i > 0:
			pdf.add_page(orientation='L')
		title = 'Top words'
		description = 'The most likely word given each topic, p(w|z), presenting the top n words for each topic'
		add_table(pdf, table=table, title=title, description=description, w=w, h=h)

	### Relevance words
	pdf.add_page(orientation='L')
	for i, table in enumerate(split_table(relevance, axis=1)):
		if i > 0:
			pdf.add_page(orientation='L')
		title = 'Relevance words'
		description = 'The most likely topic given each word, p(z|w), presenting the top n words for each topic'
		add_table(pdf, table=table, title=title, description=description, w=w, h=h)
		
	### Learnt words
	title = 'Learnt words'
	description = 'Most likely words to occur in target topic, that never co-occur with seed words.'
	
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, title)
	pdf.ln()
	pdf.set_font('Times', size=12)
	pdf.cell(w, h, description)
	pdf.ln()
	pdf.set_font('Times', size=12, style='I')
	for text in split_text(learnt):
		pdf.cell(w, h, text)
		pdf.ln()

	### Part 3
	pdf.add_page()
	add_part(pdf, 3, w, h)

	### Compile
	pdf.output(os.path.join(args.root, 'report.pdf'), 'F')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=str, default='.')
    parser.add_argument("--target_topic", type=int, default=0)
    args = parser.parse_args()
    main(args)
