import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def timeplot(df):
	with sns.axes_style("whitegrid"):
		f = sns.lineplot(x='time', y='value', data=pd.melt(df, ['time']),
						 hue='variable', style='variable',
		             	 palette=sns.color_palette('hls', n_colors=n_vars))
		sns.despine()
		f.set_title('Occurences of sex related words in DN')
		f.set(xlabel='Time')
		f.set(ylabel='Frequency')

		f.legend(labels=df.columns[1:],
			loc="center left",
			title="Word",
			bbox_to_anchor=(1, 0.5),
			ncol=3)
		
	return f

df = pd.read_csv('results/word-counts.csv')
df.rename(columns={df.columns[0]:'time'}, inplace=True)
n_vars = len(df.columns)-1

# Over time
f = timeplot(df)
plt.savefig('results/plots/word-counts.png', dpi=300, bbox_inches='tight')
plt.close('all')

# By decade
periods = np.array([list(np.arange(1900+(10*i), 1900+(10*(i+1)))) for i in range(13)])
df2 = pd.DataFrame(np.zeros((len(periods), n_vars), dtype=int), columns=df.columns[1:])
for _, row in df.iterrows():
	year = row["time"]
	idx = np.where(periods == year)[0][0]
	df2.loc[idx] += row[1:]
df2["time"] = [str(periods[i, 0]) + 's' for i in range(len(periods))]

f = timeplot(df2)
f.set_title(f.get_title() + ' by decade')
for i, label in enumerate(f.get_xticklabels()):
	if i % 2 != 0:
	    label.set_visible(False)

plt.savefig('results/plots/word-counts-decade.png', dpi=300, bbox_inches='tight')
plt.close('all')