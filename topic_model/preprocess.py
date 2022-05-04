'''
Preprocess Mallet format dataset
'''
import re
import multiprocessing
from unidecode import unidecode
from collections import Counter
from tqdm import tqdm
import fileinput
from functools import partial


def multiple_replace(dict: dict, text: str):
	regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))
	return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text) 


def clean_line(patterns, line):
	count = Counter()
	dark_id, date, text = line.replace('\n', '').split('\t')
	old_text = text
	
	text = text.lower()
	text = multiple_replace(patterns['latin_characters'], text)	
	text = re.sub(patterns['digit_pattern'], '0', text)
	text = re.sub(patterns['gram_pattern'], '_', text)
	text = re.sub(patterns['character_pattern'], '', text)
	
	# Counting words and removing excess whitespace
	text = text.split()
	count.update(text)
	text = ' '.join(text)

	line = f"{dark_id}\t{date}\t{text}\n"
	return line, count


def clean_file(patterns, file_path, clean_file_path, n_lines):
	pool = multiprocessing.Pool()
	clean_f = open(clean_file_path, 'w')
	f = open(file_path, 'r')
	count = Counter()
	for line, c in tqdm(pool.imap(partial(clean_line, patterns), f), total=n_lines):
		count += c
		clean_f.write(line)
	clean_f.close()
	f.close()
	pool.close()
	return count


def remove_rare_words_line(rare_words, line):
	dark_id, date, *text = line.split()
	text = ' '.join([w for w in text if w not in rare_words])
	return '\t'.join([dark_id, date, text])


def remove_rare_words_file(count, clean_file_path, n_lines, threshold=20):
	pool = multiprocessing.Pool()
	rare_words = [key for key, value in count.items() if value <= threshold]
	with fileinput.FileInput(clean_file_path, inplace=True) as file:
	    for line in tqdm(pool.imap(partial(remove_rare_words_line, rare_words), file), total=n_lines):
	        print(line, end='\n')
	pool.close()


def main():
	# Args
	file_path = "/media/robin/dn/dn_millenium.txt"

	# Replacement patterns
	digit_pattern = re.compile(r'\d+')
	gram_pattern = re.compile(r'(?<=[a-zåäö])-(?=[a-zåäö])')
	character_pattern = re.compile(r'[^a-zåäö0\s\_]')
	latin_characters = [chr(c) for c in range(223,225+1)] # 192:591 for all
	latin_characters = {c:unidecode(c) for c in latin_characters if c not in 'åäö'}
	patterns = {'digit_pattern':digit_pattern,
				'gram_pattern':gram_pattern,
				'character_pattern':character_pattern,
				'latin_characters':latin_characters}

	clean_file_path = file_path.replace('.txt', '_clean.txt')
	n_lines = sum(1 for line in open(file_path))
	count = clean_file(patterns, file_path, clean_file_path, n_lines)
	remove_rare_words_file(count, clean_file_path, n_lines)

if __name__ == '__main__':
	main()