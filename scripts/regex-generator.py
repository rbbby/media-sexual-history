'''
TODO: regex slightly broken with the [l|la] thing...
'''
import pandas as pd
import re

patterns =	[
	["abort",			"{}abort{}"				],
	["könssjukdom",		"aids{}"				],
	["samlag",			"{}analsex{}"			],
	["hbtq",			"{}bisex{}"				],
	["abort",			"{}fosterfördriv{}"		],
	["störning",		"{}frigid{}"			],
	["könssjukdom",		"{}gonorr[e|é]{}"		],
	["preventivmedel",	"{}gummivar{}"			],
	["hbtq",			"{}heterosex{}"			],
	["könssjukdom",		"hiv{}"					],
	["hbtq",			"{}homosex{}"			],
	["samlag",			"{}hånge(?:l|la){}"		],
	["störning",		"{}impotens{}"			],
	["könssjukdom",		"{}klamydia{}"			],
	["preventivmedel",	"{}kondom{}"			],
	["könssjukdom",		"{}könsherpes{}"		],
	["samlag",			"{}könsorgan{}"			],
	["könssjukdom",		"{}könssjukdom{}"		],
	["hbtq",			"{}masochis{}"			],
	["störning",		"{}masturb[a|e]{}"		],
	["störning",		"{}onan[i|e]{}"			],
	["sexköp",			"{}osedlig{}"			],
	["hbtq",			"{}otukt{}"				],
	["könssjukdom",		"{}p-piller{}"			],
	["hbtq",			"{}pervers{}"			],
	["preventivmedel",	"{}pessar{}"			],
	["samlag",			"{}petting{}"			],
	["porr",			"{}por(?:r|no){}"		],
	["preventivmedel",	"{}preventivmed{}"		],
	["sexköp",			"{}prostitu{}"			],
	["samlag",			"{}samlag{}"			],
	["sexköp",			"{}sexköp{}"			],
	["sexköp",			"{}sexhand{}"			],
	["sexuell",			"sexuell{}"				],
	["sexuell",			"sexual{}"				],
	["sexköp",			"{}sexualmoral{}"		],
	["sexualupplysning","{}sexualundervis{}"	],
	["sexualupplysning","{}sexualupplys{}"		],
	["preventivmedel",	"{}steriliser{}"		],
	["könssjukdom",		"{}syfilis{}"			],
	["hbtq",			"{}transsex{}"			],
	["hbtq",			"{}transvesti{}"		],
	["våldtäkt",		"{}våldt(?:a|äkt){}"	]
	]

df = pd.DataFrame(patterns, columns=['label', 'word'])
df['expression'] = pd.Series(str)

fix = r'[A-Za-zÀ-ÿ-]*' # Add more characters
for i, row in df.iterrows():
	df.loc[i, 'expression'] = row['word'].format(fix, fix)
	df.loc[i, 'word'] = re.sub(r'[^A-Za-zÀ-ÿ]', '', row['word'])

df.to_csv('data/patterns.csv', index=False)