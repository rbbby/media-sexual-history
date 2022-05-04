import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('df_valid.csv')

df = df[df['label'] == 1].reset_index(drop=True)
df = df.sort_values('probs', ascending=True).reset_index(drop=True)
for i in range(10):
	print(df.loc[i, 'id'])

def plot():
	plt.hist(df.loc[df['label'] == 0, 'probs'], bins=100, alpha=0.5, label="False")
	plt.hist(df.loc[df['label'] == 1, 'probs'], bins=100, alpha=0.5, label="True")

	plt.xlabel("Data", size=14)
	plt.ylabel("Count", size=14)
	plt.title("Multiple Histograms with Matplotlib")
	plt.legend(loc='upper right')
	#plt.show()
	plt.close()
