from torch.utils.data import DataLoader
from torch.utils.data import IterableDataset
from tqdm import tqdm
import re
from unidecode import unidecode
import argparse


def multiple_replace(text, i_start=192, i_end=383):
    d = [chr(c) for c in range(i_start, i_end + 1)]
    d = {c: unidecode(c) for c in d if c not in "åäöÅÄÖ"}
    regex = re.compile("(%s)" % "|".join(map(re.escape, d.keys())))
    return regex.sub(lambda mo: d[mo.string[mo.start() : mo.end()]], text)


class MalletDataset(IterableDataset):

    def __init__(self, filename, patterns):
        self.filename = filename
        self.patterns = patterns

    def __len__(self):
        return sum(1 for line in open(self.filename))

    def preprocess(self, line):
        patterns = self.patterns
        line = line.lower()
        line = multiple_replace(line)
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
    digit_pattern = re.compile(r'\d+')
    gram_pattern = re.compile(r'(?<=[a-zåäö])-(?=[a-zåäö])')
    character_pattern = re.compile(r'[^a-zåäö0\s\_\-]')

    # N-grams
    grams= {}
    with open('topic_model/data/n_grams.txt') as file:
        for line in file:
            line = line.replace('\n', '')
            line = line.lower()
            line = multiple_replace(line)
            line = re.sub(r'[^a-zåäö]+', '_', line)
            grams[line] = line

    patterns = {'digit_pattern':digit_pattern,
                'gram_pattern':gram_pattern,
                'character_pattern':character_pattern,
                'grams':grams}

    # Use torch to process and write text in parallell and batches
    dataset = MalletDataset(args.filename, patterns)
    dataloader = DataLoader(dataset, batch_size = args.batch_size)
    n_batches = len(dataloader)

    f = open(args.filename.replace('.txt', '_clean.txt'), 'w')
    for i, lines in enumerate(tqdm(dataloader, total=n_batches)):
        if i == n_batches-1:
            lines[-1] = lines[-1].rstrip('\n')
        f.writelines(lines)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filename", type=str)
    parser.add_argument("--batch_size", type=int, default=512)
    args = parser.parse_args()
    main(args)
