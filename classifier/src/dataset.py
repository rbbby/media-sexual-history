import torch
import os

class MediaDataset(torch.utils.data.Dataset):
    """Dataset with cropped images and text from OCR performed on newspapers."""

    def __init__(self, df):
        """
        Args:
            df (DataFrame): DataFrame with text, image path and label annotations.
            transform (callable, optional): Optional transform to be applied
                on a sample.
            mix (bool): True if model uses both global and local image features, False otherwise.
            global_features: True if only global image features are used
        """
        self.df = df
        self.tokenizer = torch.hub.load('huggingface/pytorch-transformers',
                                        'tokenizer',
                                        'KB/bert-base-swedish-cased')  # Download vocabulary from S3 and cache.

    def __len__(self):
        return len(self.df)

    def __getitem__(self, index):

        df_row = self.df.iloc[index]
        label = df_row["label"]

        # Text
        ocr_text = df_row["text"]
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