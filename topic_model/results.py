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
							PosteriorDataset,
							compute_document_topic_matrix,
							)


def main(args):
	# Config
	cfg = get_config(args.root)
	cfg['iterations'] = '2000' # for debugging
	cfg['alpha'] = '0.1' # for debugging

	# Create output folder
	out_dir = os.path.join(args.root, 'result')
	os.makedirs(out_dir, exist_ok=True)
	
	# Convergence
	p = plot_convergence(args.root).draw()
	p.savefig(f'{out_dir}/convergence.png', dpi=300)

	# Load matrices
	phi = get_phi(args.root, chunksize=args.chunksize)
	Nd = compute_document_topic_matrix(args.root, cfg)

	# Tables
	top, relevance = table_words(args.root, cfg, phi=phi, n=20, top=True, relevance=True)
	learnt = learnt_words(args.root, cfg, phi, n=20)
	top.to_csv(f'{out_dir}/top_words.csv', index=False)
	relevance.to_csv(f'{out_dir}/relevance_words.csv', index=False)
	learnt.to_csv(f'{out_dir}/learnt_words.csv', index=False)

	# Co-occurence matrix
	# Topic correlation heatmap
	fig = sns.heatmap(np.corrcoef(Nd.T))
	plt.savefig(f'{out_dir}/topic_correlation_heatmap.png')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", "-f", type=str)
    parser.add_argument("--chunksize", "-c", type=int, default=1000)
    args = parser.parse_args()
    main(args)
