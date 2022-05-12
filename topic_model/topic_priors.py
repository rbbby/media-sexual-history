'''
Topic priors from csv to Mallet format 
TODO: use spacy(?) to add all inflexions
'''
import pandas as pd
import argparse

def main(args):
	df = pd.read_csv(args.filename)
	df = df.sort_values(by=['topic_id', 'topic', 'theme', 'word']).reset_index(drop=True)

	f = open(args.filename.replace('csv', 'txt'), 'w')
	for i in sorted(set(df['topic_id'])):
		line = ', '.join(df.loc[df['topic_id'] == i, 'word'].tolist())
		f.write(f'{i}, ' + line + '\n')

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--filename", type=str, default="topic_model/data/topic_priors_1.csv")
	args = parser.parse_args()
	main(args)

