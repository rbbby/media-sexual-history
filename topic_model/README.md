## topic_model

Module for preprocessing data for seeded topic modelling and to produce files with topic model results.

### 0_preprocess.py

Cleans data and forms n-grams.

    - filename, path/to/dataset.csv

    - chunksize, number of rows to clean at a time

### 0_topic_priors.py

Generates seed word file to be used for PCPLDA.

	- filename, path/to/topic_priors.csv

	- topics, number of topics

### 1_results.py

    - root, path/to/run/directory

    - chunksize, number of rows to read at a time for _phi_

    - n, number of table rows, for example the number of top words

    - tables, no value, just add "--tables" to produce tables
