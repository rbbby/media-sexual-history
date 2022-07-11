'''
Topic priors from csv to Mallet format 
TODO: use spacy(?) to add all inflexions
'''
import pandas as pd
import argparse

def main(args):
	df = pd.read_csv(args.filename, sep=';')
	
	df = df.sort_values(by=['topic_id', 'topic', 'theme', 'word']).reset_index(drop=True)
	f = open(args.filename.replace('.csv', f'_{args.topics}.txt'), 'w')
	topics = sorted([k for k in set(df['topic_id']) if k != -1])
	for i in topics:
		line = df.loc[df['topic_id'] == i, 'word']
		line = line.str.lower()
		line = sorted(list(set(line)))
		line = ', '.join(line)
		f.write(f'{i}, ' + line + '\n')

	anti_topics = [k for k in range(args.topics) if k not in topics]
	anti_seed_words = ', '.join(df.loc[df['topic_id'] == -1, 'word'].tolist())
	for i in anti_topics:
		f.write(f'{i}, ' + anti_seed_words + '\n')

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--filename", type=str, default="topic_model/data/topic_priors_sub_sex_v1.csv")
	parser.add_argument("--topics", "-K", type=int)
	args = parser.parse_args()
	main(args)

