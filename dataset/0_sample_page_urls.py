'''
Sample pages for annotation of classifier training data and/or OCR quality
'''
import pandas as pd
import argparse
import kblab
from kblab.utils import flerge
import random
from tqdm import tqdm


def sample_issues(archive, args):
    '''
    Sample package ids
    '''
    random.seed(args.seed)
    years = range(args.start, args.end)
    dark_ids = []
    print('Sampling issues...')
    for year in tqdm(years, total=len(years)):
        packages = archive.search({'label': args.label, 'meta.created': year})
        if packages.n:
            dark_ids.extend(random.sample(packages, args.n))
    return dark_ids


def sample_pages(archive, dark_ids):
    '''
    Samples a single page from each package
    '''
    random.seed(args.seed)
    data = []
    print('Sampling pages...')
    for dark_id in tqdm(dark_ids, total=len(dark_ids)):
        p = archive.get(dark_id)
        flerged_package = flerge(package = p, level = 'Text')
        date = flerged_package[0]['meta']['created']
        pages_urls = set([block['path'][2].get('@id') for block in flerged_package])
        page_url = random.sample(list(pages_urls), 1)[0]
        data.append([date, page_url])
    return pd.DataFrame(data, columns=['date', 'page_url'])


def main(args):
    '''
    Sample n packages per year and a page from each of these issues
    '''
    with open(args.credentials, 'r') as f:
	    credentials = f.read().rstrip('\n')
    lab = ('beta' if args.betalab else 'data') + 'lab'
    a = kblab.Archive(f'https://{lab}.kb.se', auth=('demo', credentials))
    dark_ids = sample_issues(a, args)
    df = sample_pages(a, dark_ids)
    df.to_csv(args.outfile, index=False)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outfile", type=str, help="path/to/outfile") # dataset/toy_sample.csv
    parser.add_argument("--n", type=int, default=1, help="Number of pages per year")
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--start", type=int, default=1900)
    parser.add_argument("--end", type=int, default=2022)
    parser.add_argument("--credentials", type=str, help="Path to KB API credentials txt file") # /Users/username/path/to/credentials.txt
    parser.add_argument("--label", type=str, help="API label with escaped whitespace") # DAGENS\ NYHETER
    parser.add_argument('--betalab', action=argparse.BooleanOptionalAction) # to access betalab.kb.se for non copyrighted data
    args = parser.parse_args()
    main(args)
