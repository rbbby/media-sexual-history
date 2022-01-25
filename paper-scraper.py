import kblab
import os, json
from collections import Counter
import multiprocessing
import time
import re

def count(package_id):
	'''
	Get Counter of words in newspaper from package_id.
	'''
	c = Counter()
	p = a.get(package_id)
	if 'meta.json' in p:
		year = int(json.load(p.get_raw('meta.json')).get('year'))
		if year < 1900:
			return c

	if 'content.json' in p:
		for part in json.load(p.get_raw('content.json')):
			c.update(part.get('content', '').split())

	return c

# Archive needs to be loaded in every subprocess
with open(os.path.expanduser('~/credentials.txt'), 'r') as f:
		credentials = f.read()
a = kblab.Archive('https://betalab.kb.se', auth=('demo', credentials))

# This will not run / be loaded within the subprocess
def main():
	'''
	Parallellized counting of word frequencies 
	'''
	max_issues = None

	start = time.time()
	issues = {'label': 'DAGENS NYHETER'}
	c = Counter()
	with multiprocessing.Pool() as pool:
		for words in pool.imap(count, a.search(issues, max=max_issues)):
		    c.update(words)

	print(f'Finished after {round(time.time()-start, 2)} seconds')

	with open('word-counts.json', 'w') as f:
		json.dump(c, f, indent=4, ensure_ascii=False)
	
	
#if __name__ == '__main__':
#	main()

with open('word-counts.json', 'r') as f:
	c = json.load(f)

## Descriptive statistics
#print(f'No. tokens before removing rare tokens: {sum(c.values())}')
## Perform additional cleaning before removing rare words (i.e. replace(',', '') etc.)
#c = {key:value for key, value in c.items() if value > 5}
#print(f'No. tokens after removing rare tokens: {sum(c.values())}')
#print(f'Proportion of tokens with only alphabetic characters: \
#{round(sum([word.isalpha() for word in c.keys()]) / len(c), 2)}')
#print(f'Proportion of tokens including hyphen: \
#{round(len([word for word in c.keys() if "-" in word]) / len(c), 2)}')

# List of words containing special characters
pattern = re.compile(r'[^A-Za-zÀ-ÿ0-9]')
special_words = [word for word in c.keys() if bool(pattern.search(word))]
