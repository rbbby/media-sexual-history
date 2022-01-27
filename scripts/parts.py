import numpy as np
import pandas as pd
import os, json
import kblab
from http.client import IncompleteRead

with open(os.path.expanduser('~/keys/kb-credentials.txt'), 'r') as f:
	credentials = f.read().strip('\n')
a = kblab.Archive('https://datalab.kb.se', auth=('demo', credentials))

issues = {'label': 'DAGENS NYHETER'}
max_issues = None
protocols = a.search(issues, max=max_issues)

d = {}
for i, package_id in enumerate(protocols):
	
	p = a.get(package_id)
	print(i, package_id)

	if "meta.json" in p:
		meta = json.load(p.get_raw('meta.json'))
		date = meta["created"]

		c = 0
		for part in json.load(p.get_raw('structure.json')):
			if part["@type"] == 'Part':
				c += 1

		d[date] = c

with open('results/parts.json', 'w') as f:
	json.dump(d, f, indent=4)

	# problem: 14449 dark-3690065
	# problem: 16932 dark-3694960
