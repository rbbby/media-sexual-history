from torch.utils.data import DataLoader
from torch.utils.data import IterableDataset
from tqdm import tqdm
import re
from unidecode import unidecode
import argparse

def multiple_replace(dict: dict, text: str):
    regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))
    return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text) 


class MalletDataset(IterableDataset):

    def __init__(self, filename, patterns):
        self.filename = filename
        self.patterns = patterns

    def __len__(self):
        return sum(1 for line in open(self.filename))

    def preprocess(self, line):
        patterns = self.patterns
        line = line.lower()
        line = multiple_replace(patterns['latin_characters'], line)
        line = re.sub(patterns['digit_pattern'], '0', line)
        line = re.sub(patterns['character_pattern'], '', line)
        line = line.replace('-', '_')
        for key, value in patterns['grams'].items():
            line = line.replace(key, value)
        line = ' '.join(line.split())
        return line+'\n'

    def line_mapper(self, line):
        dark_id, date, text = line.split('\t')
        text = self.preprocess(text)
        return '\t'.join([dark_id, date, text])

    def __iter__(self):
        file_itr = open(self.filename)
        mapped_itr = map(self.line_mapper, file_itr)
        return mapped_itr


def main(args):
    # Replacement patterns
    grams= {}
    with open('topic_model/bi-grams.txt') as file:
        for g in file:
            g = g.replace('\n', '')
            grams[g] = re.sub(r'[^a-zåäö]+', '_', g)

    digit_pattern = re.compile(r'\d+')
    gram_pattern = re.compile(r'(?<=[a-zåäö])-(?=[a-zåäö])')
    character_pattern = re.compile(r'[^a-zåäö0\s\_]')
    latin_characters = [chr(c) for c in range(223,225+1)] # 192:591 for all
    latin_characters = {c:unidecode(c) for c in latin_characters if c not in 'åäö'}
    patterns = {'digit_pattern':digit_pattern,
                'gram_pattern':gram_pattern,
                'character_pattern':character_pattern,
                'latin_characters':latin_characters,
                'grams':grams}

    # Use torch to process and write text in parallell and batches
    dataset = MalletDataset(args.filename, patterns)
    dataloader = DataLoader(dataset, batch_size = args.batch_size)

    f = open(args.filename.replace('.txt', '_clean_test.txt'), 'w')
    for lines in tqdm(dataloader, total=len(dataloader)):
        f.writelines(lines)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filename", type=str, default='/media/robin/dn/dn.txt')
    parser.add_argument("--batch_size", type=int, default=512)
    args = parser.parse_args()
    main(args)
