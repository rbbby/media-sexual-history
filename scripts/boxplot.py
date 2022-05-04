import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import gridspec

from plotnine import (	ggplot, geom_boxplot, aes, geom_line, geom_abline, theme_void,
						ggtitle, scale_x_discrete, scale_y_discrete, geom_blank)

df = pd.read_csv('results/bert_experiment.csv')


def create_plots(df):
	assert len(set(df['n_words'])) == 1
	n_words = df.loc[0, 'n_words']

	dfp = df.copy()
	dfp.loc[dfp['n_frozen_layers'].isna(), 'n_frozen_layers'] = -2
	dfp['n_frozen_layers'] = dfp['n_frozen_layers'].astype(int).astype(str)
	dfp.loc[dfp['n_frozen_layers'] == str(-1), 'n_frozen_layers'] = 'All'
	dfp.loc[dfp['n_frozen_layers'] == str(-2), 'n_frozen_layers'] = 'None'
	order = [int(c) for c in set(dfp['n_frozen_layers']) if c not in ['None', 'All']]
	order = ['None'] + [str(c) for c in sorted(order)] + ['All']

	bp = (
		ggplot(data=dfp)
		+ aes(x="n_frozen_layers", y="accuracy", group="n_frozen_layers")
		+ geom_boxplot()
		+ scale_x_discrete(limits=order)
		+ ggtitle(f"BERT classification of news segments in DN{n_words}")
		)

	return bp

df10 = df[df['n_words'] == 10].reset_index(drop=True)
df20 = df[df['n_words'] == 20].reset_index(drop=True)

p10 = create_plots(df10)
#f10 = p10.draw()
#plt.show()
#plt.close()

p20 = create_plots(df20)
#f20 = p20.draw()
#plt.show()
#plt.close()
