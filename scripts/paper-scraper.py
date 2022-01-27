'''
TODO: - Match lowertext
	  - Regex
	  - Look at collocations
	  - Find bigrams
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

def word_counter(package_id, patterns):
	'''
	Get Counter of words in newspaper from package_id.
	'''
	c = Counter()
	p = a.get(package_id)
	if 'meta.json' in p:
		year = int(json.load(p.get_raw('meta.json')).get('year'))
		if year < 1900:
			return [c, year]

	if 'content.json' in p:
		c = {w:Counter() for w in patterns["word"]}
		for part in json.load(p.get_raw('content.json')):
			s = part.get('content', '')
			
			for _, w, exp in patterns.itertuples():
				if match := re.findall(exp, s):
					c[w].update(match)

	return [c, year]

# Archive needs to be loaded in every subprocess
with open(os.path.expanduser('~/keys/kb-credentials.txt'), 'r') as f:
	credentials = f.read().strip('\n')
a = kblab.Archive('https://datalab.kb.se', auth=('demo', credentials))

# This will not run / be loaded within the subprocess
def main():
	'''
	Parallellized counting of word frequencies 
	'''
	patterns = pd.read_csv('data/patterns.csv')
	issues = {'label': 'DAGENS NYHETER'}
	max_issues = 100

	years = range(1900, 2023)
	c = Counter()
	
	with multiprocessing.Pool() as pool:
		protocols = a.search(issues, max=max_issues)
		for count, year in pool.starmap(word_counter, zip(protocols, repeat(patterns))):
			c.update(count)

	with open('results/word-counts.json', 'w') as f:
		json.dump(c, f, indent=4, ensure_ascii=False)

if __name__ == '__main__':
	main()
