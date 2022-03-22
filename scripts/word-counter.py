'''
TODO: Clean up this very poor script...
'''
import kblab
import os, json
import numpy as np
import pandas as pd
from collections import Counter
import multiprocessing
import time
import re
from itertools import repeat
import subprocess
import matplotlib.pyplot as plt
import seaborn as sns
from itertools import product

def vis_counts(df):
	'''Does not say anything atm, need "unconditional" parts whatever that is.'''
	sns.set_style(style="whitegrid")
	f, ax = plt.subplots()

	parts = sorted(set(df['part']))
	for part in parts:
		df2 = df[df['part'] == part]
		c = Counter(df2['page'])
		x = c.keys()
		y = c.values()
		ax.bar(x, y, label=f'{part} ({str(len(df2))})')
	
	sns.despine()
	f.legend(title='Part (count)', loc="center right", bbox_to_anchor=(1, 0.5))
	ax.set(	title='Sex related word frequencies by page and part in DN',
			xlabel='Page', ylabel='Frequency')
	return f

def time_plot(df, by_part=False):
	sns.set_style(style="whitegrid")
	f, ax = plt.subplots()
	title = 'Sex related word frequencies over time in DN'
	
	if by_part == False:
		x = df['date'].str[:4]
		y = np.ones(len(x))
		df2 = pd.DataFrame({'year':x, 'frequency':y})
		series = df2.groupby(x)['frequency'].sum()
		ax.plot(series.index, series, label='Part')
	
	if by_part == True:
		title += ' by part'
		parts = set(df['part'])
		for part in parts:
			df2 = df[df['part'] == part]
			x = df2['date'].str[:4]
			y = np.ones(len(x))
			df2 = pd.DataFrame({'year':x, 'frequency':y})
			series = df2.groupby(x)['frequency'].sum()
			ax.plot(series.index, series, label=part)
	
	sns.despine()
	f.legend(title='Part', loc="center right", bbox_to_anchor=(1.1, 0.5))
	ax.xaxis.set_major_locator(plt.MaxNLocator(12))
	ax.set(	title=title, xlabel='Year', ylabel='Frequency')
	return f

def labels_over_time(df, time, relative=False):
	labels = sorted(set(df['label']))	
	sns.set_style(style='whitegrid')
	f, ax = plt.subplots()
	ax.set(xlim=(1900, 2021))
	plt.xticks(list(range(1900, 2020+1, 20)))
	for label in labels:
		x = pd.crosstab(df[time], df['label'], normalize='index' if relative else False)	
		ax.plot(x.index, x[label], label=label)
	sns.despine()
	f.legend(title='Part', loc="center right", bbox_to_anchor=(1.15, 0.5))
	return f, ax

def words_over_time(df, time, relative=False):
	words = sorted(set(df['word']))	
	sns.set_style(style='whitegrid')
	f, ax = plt.subplots()
	ax.set(xlim=(1900, 2021))
	plt.xticks(list(range(1900, 2020+1, 20)))
	for word in words:
		x = pd.crosstab(df[time], df['word'], normalize='index' if relative else False)	
		ax.plot(x.index, x[word], label=word)
	sns.despine()
	f.legend(title='Part', loc="center right", bbox_to_anchor=(1.15, 0.5))
	return f, ax

# Grouped plots
def grouped_plots():
	parts = list(sorted(set(df['part'])))[:5]
	parts = [parts] + [[part] for part in parts]
	for part in parts:
		for t, rel in combos:
			dfp = df[df['part'].apply(lambda x: x in part)]
			f, ax = labels_over_time(dfp, t, relative=rel)
			ylab = 'Proportion' if rel else 'Frequency'
			title = f'{ylab} of sex related words in DN by {t} in part {part}'
			ax.set(title=title, xlabel=t, ylabel=ylab)
			os.makedirs(f'results/plots/word-counts/aggregated', exist_ok = True)
			partlab = f'_part_{part[0]}' if len(part) == 1 else ''
			plt.savefig(f'results/plots/word-counts/aggregated/labels{partlab}_by_{t}_{ylab[:4].lower()}.png', dpi=300, bbox_inches='tight')
			plt.close()

# Grouped plots
def grouped_plots_riksdagen():
	for t, rel in combos:
		f, ax = labels_over_time(df, t, relative=rel)
		ylab = 'Proportion' if rel else 'Frequency'
		title = f'{ylab} of sex related words in DN by {t}'
		ax.set(title=title, xlabel=t, ylabel=ylab)
		os.makedirs(f'results/plots/word-counts/riksdagen/aggregated', exist_ok = True)
		plt.savefig(f'results/plots/word-counts/riksdagen/aggregated/labels_by_{t}_{ylab[:4].lower()}.png', dpi=300, bbox_inches='tight')
		plt.close()

def label_plots():
	labels = list(sorted(set(df['label'])))
	for label in labels:
		for t, rel in combos:
			dfp = df[df['label'].apply(lambda x: x in label)]
			f, ax = words_over_time(dfp, t, relative=rel)
			ylab = 'Proportion' if rel else 'Frequency'
			title = f'{ylab} of sex related words in DN by {t} in category {label}'
			ax.set(title=title, xlabel=t, ylabel=ylab)
			os.makedirs(f'results/plots/word-counts/riksdagen/{label}', exist_ok = True)
			plt.savefig(f'results/plots/word-counts/riksdagen/{label}/label_{label}_by_{t}_{ylab[:4].lower()}.png', dpi=300, bbox_inches='tight')
			plt.close()
			print(f'label_{label}_by_{t}_{ylab[:4].lower()}.png')

patterns = pd.read_csv('data/patterns.csv')
mode = 'riksdagen'

if mode == 'dn':
	path = '/media/robin/data/dn-1900-present'
	df = pd.read_csv(os.path.join(path, 'dn.csv'))
	df['label'] = df['label'].replace({'störning':'onani', 'sexualupplysning':'upplysning'})
	df['year'] = df['date'].apply(lambda x: int(x[:4]))
	df = df[df['year'] >= 1900].reset_index(drop=True)
	df['5_year'] = df['year'].apply(lambda x: int(str(x)[:3] + str((int(str(x)[-1]) >= 5) * 5)))
	combos = list(product(['year', '5_year'], [True, False]))
	grouped_plots()
	label_plots()

if mode == 'riksdagen':
	df = pd.read_csv('data/riksdagen.csv')
	df['year'] = df['prot'].apply(lambda x: x.split('-')[1][:4])
	df['5_year'] = df['year'].apply(lambda x: int(str(x)[:3] + str((int(str(x)[-1]) >= 5) * 5)))
	combos = list(product(['5_year'], [True, False]))
	grouped_plots_riksdagen()
	label_plots()
