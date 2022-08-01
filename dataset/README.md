## Dataset

### 0_sample_page_urls

Used to sample page urls for annotating training data and/or data for examining OCR quality

### 1_preprocess.py

The annotated training data will be in many small files. This script joins them together and labels unlabeled observations as 0. Note that depending on how you annotate data you may have to remap labeled observations depending on the classification task.