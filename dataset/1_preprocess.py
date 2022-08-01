import re
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
import kblab
from kblab.utils import flerge

import argparse
from operator import itemgetter
from pathlib import Path

def main(args):
    # Load credentials
    with open(args.credentials, 'r') as f:
	    credentials = f.read().rstrip('\n')
    lab = ('beta' if args.betalab else 'data') + 'lab'
    a = kblab.Archive(f'https://{lab}.kb.se', auth=('demo', credentials))

    # Load data
    path = Path(args.annotations)
    df = pd.concat([pd.read_csv(f) for f in path.glob('**/*.csv')]).reset_index(drop=True)

    # Get helper variables for filtering data
    df['page_url'] = df['id'].apply(lambda x: '-'.join(x.split('-')[:3]))
    df['dark_id'] = df['id'].str.extract(r'(dark-[0-9]+)')

    if args.impute:
        # Iterate over unique issues
        data = []
        for dark_id, dark_df in df.groupby('dark_id'):
            page_urls = set(dark_df['page_url'].tolist())
            ids = set(dark_df['id'].tolist())
            p = a.get(dark_id)
            flerged_package = flerge(package = p, level = 'Text')

            # Store textblocks in annotated pages that are not yet annotated
            for row in flerged_package:
                # Skip if not from sampled page
                if not row['path'][2].get('@id') in page_urls:
                    continue
                # Skip if not textblock
                if row['@type'] != 'Text':
                    continue
                # Skip if we already have annotated this observation
                if row['@id'] in ids:
                    continue
                
                
                id, type, box, content = itemgetter('@id', '@type', 'box', 'content')(row)
                tag = 0 # label all textblocks as 0
                page_url = '-'.join(id.split('-')[:3])            
                dark_id = re.search(r'(dark-[0-9]+)', id).group(0)
                data.append([id, type, tag, box, content, page_url, dark_id])
            
            df = pd.concat([df, pd.DataFrame(
                data, columns=['id', 'type', 'tag', 'box', 'content', 'page_url', 'dark_id'])])    
        
    df = df.merge(pd.read_csv(args.sample), on='page_url', how='left')
    df.to_csv(args.outfile, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outfile", type=str) # path/to/outfile
    parser.add_argument("--sample", type=str) # path to file generated by dataset/sample_page_urls
    parser.add_argument("--annotations", type=str, default='dataset/annotations') # folder with annotated pages
    parser.add_argument("--credentials", type=str, help="Path to KB API credentials txt file") # /Users/username/path/to/credentials.txt
    parser.add_argument('--betalab', action=argparse.BooleanOptionalAction) # to access betalab.kb.se for non copyrighted data
    parser.add_argument('--impute', action=argparse.BooleanOptionalAction) # imputes data
    args = parser.parse_args()
    main(args)

