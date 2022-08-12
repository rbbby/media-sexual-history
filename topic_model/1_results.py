import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import re
from pathlib import Path
import multiprocessing
from tqdm import tqdm
import argparse
from PCPLDA.results import (get_config, get_vocab, get_phi, get_seed_words,
							table_words,
							plot_convergence,
							learnt_words,
							get_metadata,
							compute_document_topic_matrix,
							)


def main(args):
	# Config
	cfg = get_config(args.root)

	# Create output folder
	out_dir = os.path.join(args.root, 'result')
	os.makedirs(out_dir, exist_ok=True)
	
	# Convergence
	print('Plotting convergence')
	p = plot_convergence(args.root).draw()
	p.savefig(f'{out_dir}/convergence.png', dpi=300)

	# Load matrices
	print('Computing document term matrix')
	Nd = compute_document_topic_matrix(args.root, cfg)

	# Tables
	if args.tables:
		print('Computing phi')
		phi = get_phi(args.root, chunksize=args.chunksize)
		print('Producing tables')
		top, relevance = table_words(args.root, cfg, phi=phi, n=args.n, top=True, relevance=True)
		learnt = learnt_words(args.root, cfg, phi, n=args.n)
		top.to_csv(f'{out_dir}/top_words.csv', index=False)
		relevance.to_csv(f'{out_dir}/relevance_words.csv', index=False)
		learnt.to_csv(f'{out_dir}/learnt_words.csv', index=False)

	# Co-occurence matrix
	# Topic correlation heatmap
	print('Creating heatmap')
	fig = sns.heatmap(np.corrcoef(Nd.T))
	plt.savefig(f'{out_dir}/topic_correlation_heatmap.png')

	# Get dates
	print('Getting dates')
	_, dates = get_metadata(cfg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", "-f", type=str)
    parser.add_argument("--chunksize", "-c", type=int, default=1000)
    parser.add_argument("--n", type=int, default=20)
    parser.add_argument('--tables', action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    main(args)
