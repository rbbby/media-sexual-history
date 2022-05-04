import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from tqdm import tqdm
from plotnine import ggplot, aes, geom_line, geom_abline, ggtitle
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, roc_curve
from src.dataset import *
from src.evaluation import *
from src.models import *
import transformers
import pickle
import re

def prepare_data(df):
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['weekday'] = df['date'].dt.day_of_week

    df = df.rename(columns={'tag':'label', 'content':'text'})
    df.loc[df['label'] != 1, 'label'] = 0
    df["label"] = df["label"].astype("int8")
    df = df.sample(frac=1, random_state=123)
    n_split = round(len(df) * split_ratio)
    df_train = df.iloc[:n_split].copy() # When using full page model
    df_valid = df.iloc[n_split:].copy()
    dataset_train = MediaDataset(df=df_train)
    dataset_valid = MediaDataset(df=df_valid)
    dataloader = torch.utils.data.DataLoader(
        dataset_train, batch_size=16, shuffle=True, num_workers=4)
    testloader = torch.utils.data.DataLoader(
        dataset_valid, batch_size=8, shuffle=False, num_workers=4)
    return df_train, df_valid, dataloader, testloader

def train(df_train, df_valid, dataloader, testloader):
    # Model
    model = BertMetaClassifier()
    for name, param in model.bert.named_parameters():
        if n_frozen_layers is None:
            break
        param.requires_grad = False
        if n_frozen_layers == -1:
            break
        if 'pooler' in name:
            param.requires_grad = True
        if match := re.search(r'(\d+)', name):
            if int(match.group(1)) >= n_frozen_layers:
                param.requires_grad = True
    model.to("cuda")

    # Configuration
    loss_fn = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(
        filter(lambda p: p.requires_grad, model.parameters()), lr=0.00002)
    num_training_steps = len(dataloader) * n_epochs
    num_warmup_steps = num_training_steps // 10
    scheduler = transformers.get_scheduler(
                    "linear",    
                    optimizer = optimizer,
                    num_warmup_steps = num_warmup_steps,
                    num_training_steps = num_training_steps
                    )

    # Train
    lrs = []
    valid_losses = []
    train_losses = []
    best_accuracy = 0

    for epoch in range(n_epochs):
        print(f"epoch: {epoch + 1} started")
        train_loss = 0
        for i, batch in enumerate(tqdm(dataloader)):

            texts = batch[0]
            labels = batch[1].to("cuda")
            numeric_features = batch[2]
            categorical_features = batch[3]

            optimizer.zero_grad()
            output = model(token_ids=texts["input_ids"].squeeze(dim=1).to("cuda"),
                           type_ids=texts["token_type_ids"].squeeze(
                               dim=1).to("cuda"),
                           mask=texts["attention_mask"].squeeze(dim=1).to("cuda"),
                           numeric_features=numeric_features.to("cuda"),
                           categorical_features=categorical_features.to("cuda"))

            labels = labels.unsqueeze(1).type_as(output)
            loss = loss_fn(output, labels)
            train_loss += loss.item()

            loss.backward()
            optimizer.step()
            scheduler.step()
            lrs.append(optimizer.param_groups[0]["lr"])

        # Predict
        df_valid, valid_loss = predict(df_valid, testloader, model=model)
        metrics = get_metrics(df_valid)
        valid_losses.append(valid_loss)
        train_losses.append(train_loss)

        print(f'\nTraining Loss: {train_loss:.3f}')
        print(f'Validation Loss: {valid_loss:.3f}')

        if metrics['accuracy'] > best_accuracy:
            best_accuracy = metrics['accuracy']
            best_epoch = epoch+1

        # Store model checkpoint
        if valid_loss < best_valid_loss:
            torch.save({
                'epoch': epoch+1,
                'n_frozen_layers': n_frozen_layers,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'loss': loss_fn,
                }, f'best_model_{n_words}.pth')
    return best_accuracy, best_epoch

# Args
n_epochs = 20
split_ratio = 0.7
evaluation_metric = 'accuracy'
schedule = 'triangular'
best_valid_loss = float('inf')

data = []

for n_words in [10, 20]:
    # Data
    df = pd.read_csv(f"/media/robin/dn/training-data-{n_words}.csv")
    df_train, df_valid, dataloader, testloader = prepare_data(df)
    for n_frozen_layers in [-1, 12, 11, 10, 9, 8, 6, 4, 2, 0, None]:
        for i in range(50):
            accuracy, epoch = train(df_train, df_valid, dataloader, testloader)
            data.append([n_words, n_frozen_layers, accuracy, epoch])
            print(f'Iteration {i} finished for n_frozen_layers:{n_frozen_layers} n_words:{n_words}')
df = pd.DataFrame(data, columns=['n_words', 'n_frozen_layers', 'accuracy', 'epoch'])
df.to_csv('bert_experiment.csv', index=False)