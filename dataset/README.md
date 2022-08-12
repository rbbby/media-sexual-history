## dataset

Module to generate samples and processing annotated data.

### 0_sample_page_urls

Used to sample page urls for annotating training data and/or data for examining OCR quality.

	- outfile, path/to/sample.csv
    
    - n, number of pages to sample per year
    
    - seed, e.g. 123
    
    - start, e.g. 1920
    
    - end, e.g. 2022
    
    - credentials, path/to/kblab_credentials.txt
    
    - label, name of issue at the kblab API, spaces should be escaped by writing out a backspace before them, for 
    example: "DAGENS\\ NYHETER"
    
    - betalab, no value, just add "--betalab" to access non copyrighted data at betalab rather than datalab

### 1_preprocess.py

The annotated training data will be in many small files. This script joins them together and labels unlabeled observations as 0. Note that depending on how you annotate data you may have to remap labeled observations depending on the classification task.

	- outfile, path/to/dataset.csv
	
	- outfile, path/to/sample.csv
	
	- annotations, path/to/folder/with/annotations
	
	- credentials, path/to/kblab_credentials.txt
    
    - betalab, no value, just add "--betalab" to access non copyrighted data at betalab rather than datalab
    
    - impute, no value, just add "--impute" to impute missing labels with 0