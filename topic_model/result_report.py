import numpy as np
import pandas as pd
import os
import re
import matplotlib.pyplot as plt
from pathlib import Path
import argparse
from torch.utils.data import IterableDataset, DataLoader
from PCPLDA.results import (get_config, get_vocab, get_phi, get_seed_words,
							table_words,
							plot_convergence,
							learnt_words)
from fpdf import FPDF
from itertools import count

def add_part(pdf, counter, w, h):
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, f'Part {next(counter)}', border = 0, ln = 1)
	pdf.ln()

def add_plot(pdf, plot, title, description, w, h):
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, title, ln=1)
	pdf.set_font('Times', size=12)
	pdf.cell(w, h, description, ln=1)
	pdf.ln()
	pdf.image(plot, x=50, w=pdf.w/2.0)

def output_df_to_pdf(pdf, df):
	table_cell_width = 25
	table_cell_height = 6
	pdf.set_font('Arial', 'B', 8)

	for col in df.columns:
		pdf.cell(table_cell_width, table_cell_height, str(col), align='C', border=1)
	pdf.ln(table_cell_height)
	pdf.set_font('Arial', '', 10)
	for _, row in df.iterrows():
		for col in df.columns:
			value = str(row[col])
			pdf.cell(table_cell_width, table_cell_height, value, align='C', border=1)
		pdf.ln(table_cell_height)

def add_table(pdf, table, title, description, w, h):
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, title, border = 0, ln = 1)
	pdf.set_font('Times', size=12)
	pdf.cell(w, h, description, border = 0, ln = 1)
	output_df_to_pdf(pdf, table)
	pdf.ln()

def main(args):

	args.root = '../PartiallyCollapsedLDA/Runs/RunSuite2022-06-02--21_34_10/Run2022-06-02--21_34_10'

	# Produce results
	cfg = get_config(args.root)
	p = plot_convergence(args.root).draw()
	p.savefig('convergence.png', dpi=300)
	top = table_words(args.root, cfg, top_words=True)
	relevance = table_words(args.root, cfg, top_words=False)
	learnt = learnt_words(args.root, cfg, args.target_topic)

	# Result report
	w, h = 40, 5

	# 1. Set up the PDF doc basics
	pdf = FPDF()
	pdf.add_page()
	counter = count(1)

	### Document title
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, f"PCLDA result report {os.path.split(args.root)[-1]}", ln=1)
	pdf.ln()
	add_part(pdf, counter, w, h)

	### Config
	pdf.set_font('Times', size=12, style='B')
	pdf.cell(w, h, 'Config', ln=1)
	pdf.set_font('Times', size=12)
	for key, value in cfg.items():
		pdf.cell(w, h, f"{key}: {value}", ln=1)
	pdf.ln()

	### Convergence
	title = 'Convergence'
	description = 'The evaluation function should reach some stable value that it jumps around randomly.'
	plot = 'convergence.png'
	add_plot(pdf, plot, title, description, w, h)
	pdf.add_page()

	### Part 2
	add_part(pdf, counter, w, h)

	### Top words
	title = 'Top words'
	description = 'The most likely word given each topic, p(w|z), presenting the top n words for each topic'
	add_table(pdf, table=top, title=title, description=description, w=w, h=h)

	### Relevance words
	title = 'Relevance words'
	description = 'The most likely topic given each word, p(z|w), presenting the top n words for each topic'
	add_table(pdf, table=top, title=title, description=description, w=w, h=h)
	
	### Learnt words
	title = 'Learnt words'
	description = 'Most likely words to occur in target topic, that never co-occur with seed words.'
	add_table(pdf, table=learnt, title=title, description=description, w=w, h=h)

	### Compile
	pdf.output(os.path.join(args.root, 'report.pdf'), 'F')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=str, default='.')
    parser.add_argument("--target_topic", type=int, default=0)
    args = parser.parse_args()
    main(args)
