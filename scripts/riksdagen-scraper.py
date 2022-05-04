import pandas as pd
import os
import re
from lxml import etree
from pyparlaclarin.read import speech_iterator
from tqdm import tqdm
import multiprocessing

def speech_iterator(root):
    """
    Convert Parla-Clarin XML to an iterator of speeches (ignoring any notes).

    Args:
        root: Parla-Clarin document root, as an lxml tree root.
    Return:
        speaker: corpus person_id.
        n: introduction hash.
        speech: concatenated consequtive speech segments by same speaker.
    """
    speaker = None
    n = None
    speech = []
    first_speech = True
    for body in root.findall(".//{http://www.tei-c.org/ns/1.0}body"):
        for div in body.findall("{http://www.tei-c.org/ns/1.0}div"):
            for elem in div:
                if elem.get('type') == 'speaker':
                    # Create output object
                    out = [speaker, n, ' '.join(' '.join(speech).replace('\n', '').split())]
                    n = elem.get('n')
                    speech = []
                    if not first_speech:
                        yield out
                    first_speech = False

                if elem.tag[-1] == 'u':
                    speaker = elem.get('who')
                    speech.extend(elem.itertext())

def text_scraper(path_protocol):
	root = etree.parse(path_protocol, parser).getroot()
	speeches = speech_iterator(root)
	data = []
	for person_id, n, speech in speeches:
		for i in range(len(patterns)):
			if match := re.findall(patterns.loc[i, 'expression'], speech):
				word, label = patterns.loc[i, ['word', 'label']]
				prot = os.path.split(path_protocol)[-1]
				data.append([prot, speech, word, label, match])
	return pd.DataFrame(data, columns=['prot', 'text', 'word', 'label', 'match'])

patterns = pd.read_csv('data/patterns.csv')
parser = etree.XMLParser(remove_blank_text=True)
path = '../riksdagen-corpus/corpus/protocols'

def main():
	folders = os.listdir(path)
	data = []
	for folder in tqdm(folders, total=len(folders)):
		protocols = os.listdir(os.path.join(path, folder))
		path_protocols = [os.path.join(path, folder, p) for p in protocols]
		with multiprocessing.Pool() as pool:
			for df in pool.imap(text_scraper, path_protocols):
				data.append(df)
	df = pd.concat(data)
	df.to_csv('results/riksdagen.csv', index=False)

if __name__ == '__main__':
	main()
	






