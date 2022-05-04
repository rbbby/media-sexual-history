import torch
import torch.nn as nn
from torchvision import models

class BertClassifier(nn.Module):
    """
    Text - model.
    """

    def __init__(self, pretrained=True):
        super().__init__()  # Initialize superclass nn.Module
        if pretrained:
            self.bert = torch.hub.load('huggingface/pytorch-transformers',
                                       'model',
                                       'KB/bert-base-swedish-cased',
                                       output_hidden_states=True)
        else:
            # Load saved models from disk
            pass

        self.fc = nn.Sequential(
            nn.Linear(768, 512),
            nn.ReLU(),
            nn.Dropout(p=0.3),
            nn.Linear(512, 1)
        )

    def forward(self, token_ids, type_ids, mask):

        hidden_states = self.bert(
            token_ids, token_type_ids=type_ids, attention_mask=mask)

        output = self.fc(hidden_states[0][:, 0, :])

        return output

class BertMetaClassifier(nn.Module):
    """
    Text + Metadata - model.
    """
    def __init__(self, pretrained=True):
        super().__init__()  # Initialize superclass nn.Module
        if pretrained:
            self.bert = torch.hub.load('huggingface/pytorch-transformers',
                                       'model',
                                       'KB/bert-base-swedish-cased',
                                       output_hidden_states=True)
        else:
            # Load saved models from disk
            pass

        # Bert + categorical + numeric - embeddings
        self.fc = nn.Sequential(
            nn.Linear(768 + 3 + 1, 512),
            nn.ReLU(),
            nn.Dropout(p=0.3),
            nn.Linear(512, 1)
        )

        self.categorical_embedder = nn.Embedding(
            8, 3, padding_idx=0)  # weekday embedding

    def forward(self, token_ids, type_ids, mask, numeric_features, categorical_features):

        hidden_states = self.bert(
            token_ids, token_type_ids=type_ids, attention_mask=mask)
        categorical_embedding = self.categorical_embedder(
            categorical_features).squeeze_(dim=1)

        # (1, 1408) (image) + (1, 768) (text) + (1, 4) + (1, 3)
        combined_embedding = torch.cat(
            [hidden_states[0][:, 0, :],
             numeric_features,
             categorical_embedding], dim=1)

        output = self.fc(combined_embedding)

        return output