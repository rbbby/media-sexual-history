## classifier

Train classifier for body text detection in newspapers and to use it for online predictions where only positive labels are stored locally.

### 0_train.py

Trains a classifier utilizing text and metadata including OCR coordinates, weekday and year.

	- model_filename, path/to/model.pth
	
	- device, cuda if gpu, else cpu
	
	- filename, path/to/dataset.csv
	
	- seed, e.g. 123
	
	- n_epochs, maximum number of passes through the training data
	
	- batch_size, number of observations to use in each mini-batch during training
	
	- num_workers, number of workers used to move data from the loader to device
	
	- learning_rate, gradient step size during training
	
	- train_ratio, fraction of data to be used for training, e.g. 0.6 (whichever proportion not used for training/validation is used for testing)

	- valid_ratio, fraction of data to be used for training, e.g. 0.2 (whichever proportion not used for training/validation is used for testing)

### 1_predict_online.py

Generates a dataset of only positive predictions from a model.

	- start, e.g. 1900

	- end, e.g. 2022

	- model_filename, path/to/model.pth

    - outfile, path/to/resulting_dataset.csv

    - device, cuda if gpu, else cpu

    - credentials, path/to/kblab_credentials.txt
