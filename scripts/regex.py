'''
TODO: regex slightly broken with the [l|la] thing...
'''
import pandas as pd
import re

patterns = [
	"{}abort{}",
	"aids{}",
	"{}analsex{}",
	"{}bisex{}",
	"{}fosterfördriv{}",
	"{}frigid{}",
	"{}gonorr[e|é]{}",
	"{}gummivar{}",
	"{}heterosex{}",
	"hiv{}",
	"{}homosex{}",
	"{}hånge[l|la]{}",
	"{}impotens{}",
	"{}klamydia{}",
	"{}kondom{}",
	"{}könsherpes{}",
	"{}könsorgan{}",
	"{}könssjukdom{}",
	"{}masochis{}",
	"{}masturb[a|e]{}",
	"{}onan[i|e]{}",
	"{}osedlig{}",
	"{}otukt{}",
	"{}p-piller{}",
	"{}pervers{}",
	"{}pessar{}",
	"{}petting{}",
	"{}por[r|no]{}",
	"{}preventivmed{}",
	"{}prostitu{}",
	"{}samlag{}",
	"{}sexköp{}",
	"{}sexhand{}",
	"sexuell{}",
	"sexual{}",
	"{}sexualmoral{}",
	"{}sexualundervis{}",
	"{}sexualupplys{}",
	"{}steriliser{}",
	"{}syfilis{}",
	"{}transsex{}",
	"{}transvesti{}",
	"{}våldt[a|äkt]{}"
]

fix = r'[A-Za-zÀ-ÿ-]*'
d = {re.sub(r'[^A-Za-zÀ-ÿ]', '', exp):exp.format(fix, fix) for exp in patterns}
df = pd.DataFrame(d.items(), columns=['word', 'expression'])
df.to_csv('data/patterns.csv', index=False)