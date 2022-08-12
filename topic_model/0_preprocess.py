import pandas as pd
from functools import partial
from unidecode import unidecode
import re
import random
import argparse
from tqdm import tqdm
import os


def multiple_replace(text, grams=None, i_start=192, i_end=383):
    d = [chr(c) for c in range(i_start, i_end + 1)]
    d = {c: unidecode(c) for c in d if c not in "åäö"}
    if grams:
        d = {**d, **grams}
    regex = re.compile("(%s)" % "|".join(map(re.escape, d.keys())))
    return regex.sub(lambda mo: d[mo.string[mo.start() : mo.end()]], text)


def process_text(text, grams=None):
    text = text.lower()
    text = text.replace("-", "_")
    text = multiple_replace(text, grams)
    text = re.sub(r"[^a-zåäö\s\_]", "", text)
    text = text.split()
    text = [c.strip("_") for c in text if len(c) > 1 and len(c) < 50]
    random.shuffle(text)
    text = " ".join(text)
    return text


def main(args):
    grams = {}
    with open("topic_model/data/n_grams.txt") as file:
        for line in file:
            text = line
            text = text.replace("\n", "")
            text = text.lower()
            text = multiple_replace(text)
            text = re.sub(r"[^a-zåäö]+", "_", text)
            grams[line] = text
    process_fun = partial(process_text, grams=grams)

    # Setup progress bar total
    with open(args.filename) as f:
        n = sum(1 for line in f)

    # Prepare outfile
    outfile = args.filename.replace(".txt", "_clean.txt")
    try:
        os.remove(outfile)
    except OSError:
        pass

    # Reader and writer
    with open(outfile, "w") as writer, pd.read_csv(
        args.filename, sep="\t", header=None, chunksize=args.chunksize
    ) as reader:
        # Clean and write data in chunks
        for chunk in tqdm(reader, total=n // args.chunksize):
            chunk.iloc[:, 2] = chunk.iloc[:, 2].apply(process_fun)
            txt = ["\t".join(row) for row in chunk.values.tolist()]
            writer.write("\n".join(txt))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filename", "-f", type=str)
    parser.add_argument("--chunksize", "-c", type=int, default=1000)
    args = parser.parse_args()
    main(args)
