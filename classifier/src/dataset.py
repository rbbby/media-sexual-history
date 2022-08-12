import torch
import os

class MediaDataset(torch.utils.data.Dataset):
    def __init__(self, df):
        self.df = df
        self.tokenizer = torch.hub.load('huggingface/pytorch-transformers',
                                        'tokenizer',
                                        'KB/bert-base-swedish-cased')
        
    def __len__(self):
        return len(self.df)

    def __getitem__(self, index):

        df_row = self.df.iloc[index]
        label = df_row["tag"]

        # Text
        ocr_text = df_row["content"]
        token_info = self.tokenizer.encode_plus(ocr_text,
                                                max_length=64,
                                                truncation=True,
                                                padding="max_length",
                                                return_tensors="pt")
    
        # Numeric features
        numeric_features = torch.Tensor([df_row["year"]])

        # Categorical features
        categorical_features = torch.LongTensor([df_row["weekday"]])

        return token_info, label, numeric_features, categorical_features