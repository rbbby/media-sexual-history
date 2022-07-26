'''
Sample pages for annotation of classifier training data and/or OCR quality
'''
import pandas as pd
import argparse
import kblab
from kblab.utils import flerge
import random


def sample_issues(archive, args):
    random.seed(args.seed)
    years = range(args.start, args.end)
    dark_ids = []
    for year in years:
        # SWITCH TO datalab        
        packages = archive.search({'label': args.label, 'meta.created': year})
        if packages.n:
            dark_ids.extend(random.sample(packages, args.n))
    return dark_ids


def sample_pages(archive, dark_ids):
    random.seed(args.seed)
    data = []
    for dark_id in dark_ids:
        p = archive.get(dark_id)
        flerged_package = flerge(package = p, level = 'Text')
        date = flerged_package[0]['meta']['created']
        pages_urls = set([block['path'][2].get('@id') for block in flerged_package])
        page_url = random.sample(list(pages_urls), 1)
        data.append([date, page_url])
    return pd.DataFrame(data, columns=['date', 'page_url'])


def main(args):
    with open(args.credentials, 'r') as f:
	    credentials = f.read().rstrip('\n')
    lab = ('beta' if args.betalab else 'data') + 'lab'
    a = kblab.Archive(f'https://{lab}.kb.se', auth=('demo', credentials))
    dark_ids = sample_issues(a, args)
    df = sample_pages(a, dark_ids)
    df.to_csv('classifier/sample.csv', index=False)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=1, help="Number of pages per year")
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--start", type=int, default=1900)
    parser.add_argument("--end", type=int, default=2022)
    parser.add_argument("--credentials", type=str, help="Path to KB API credentials txt file") # /Users/username/path/to/credentials.txt
    parser.add_argument("--label", type=str, help="API label with escaped whitespace") # DAGENS\ NYHETER
    parser.add_argument('--betalab', action=argparse.BooleanOptionalAction) # to access betalab.kb.se for non copyrighted data
    args = parser.parse_args()
    main(args)
