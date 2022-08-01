import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from tqdm import tqdm
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, roc_curve
from src.dataset import MediaDataset
from src.evaluation import predict, get_metrics
from src.models import BertMetaClassifier
import transformers
import argparse


def prepare_data(args):
    df = pd.read_csv(args.filename)

    # Add year and weekday variables to be used in the model
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['weekday'] = df['date'].dt.day_of_week

    # Convert outcome to binary
    df.loc[df['tag'] != 1, 'tag'] = 0

    # Splits datasets and create dataloaders
    df_train, df_valid, df_test = \
              np.split(df.sample(frac=1, random_state=args.seed), 
                       [int(args.train_ratio*len(df)), int((args.train_ratio+args.valid_ratio)*len(df))])
    dataset_train = MediaDataset(df=df_train)
    dataset_valid = MediaDataset(df=df_valid)
    dataset_test = MediaDataset(df=df_test)
    data_loader = torch.utils.data.DataLoader(
        dataset_train, batch_size=args.batch_size, shuffle=True, num_workers=args.num_workers)
    valid_loader = torch.utils.data.DataLoader(
        dataset_valid, batch_size=args.batch_size, shuffle=False, num_workers=args.num_workers)
    test_loader = torch.utils.data.DataLoader(
        dataset_test, batch_size=args.batch_size, shuffle=False, num_workers=args.num_workers)
    return df_train, df_valid, df_test, data_loader, valid_loader, test_loader


def train(args, df_train, df_valid, dataloader, valid_loader):
    # Model
    model = BertMetaClassifier().to(args.device)
    
    # Configuration
    loss_fn = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(
        filter(lambda p: p.requires_grad, model.parameters()), lr=args.learning_rate)
    num_training_steps = len(dataloader) * args.n_epochs
    num_warmup_steps = num_training_steps // 10
    # Linear warmup and step decay
    scheduler = transformers.get_scheduler(
                    "linear",    
                    optimizer = optimizer,
                    num_warmup_steps = num_warmup_steps,
                    num_training_steps = num_training_steps
                    )

    # Train
    valid_losses = []
    train_losses = []
    best_valid_loss = float('inf')

    for epoch in range(args.n_epochs):
        print(f"epoch: {epoch + 1} started")
        train_loss = 0
        for i, batch in enumerate(tqdm(dataloader)):

            texts = batch[0]
            labels = batch[1].to(args.device)
            numeric_features = batch[2]
            categorical_features = batch[3]

            optimizer.zero_grad()
            output = model(token_ids=texts["input_ids"].squeeze(dim=1).to(args.device),
                           type_ids=texts["token_type_ids"].squeeze(
                               dim=1).to(args.device),
                           mask=texts["attention_mask"].squeeze(dim=1).to(args.device),
                           numeric_features=numeric_features.to(args.device),
                           categorical_features=categorical_features.to(args.device))

            labels = labels.unsqueeze(1).type_as(output)
            loss = loss_fn(output, labels)
            train_loss += loss.item()

            loss.backward()
            optimizer.step()
            scheduler.step()

        # Predict
        df_valid, valid_loss = predict(df_valid, valid_loader, model=model, device=args.device)
        #metrics = get_metrics(df_valid)
        
        valid_losses.append(valid_loss)
        train_losses.append(train_loss)

        print(f'\nTraining Loss: {train_loss:.3f}')
        print(f'Validation Loss: {valid_loss:.3f}')
        #print(metrics)

        # Store best model
        if valid_loss < best_valid_loss:
            torch.save({
                'epoch': epoch+1,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'loss': loss_fn,
                }, args.model_filename)


def main(args):
    # Args
    df_train, df_valid, df_test, dataloader, valid_loader, test_loader = prepare_data(args)
    train(args, df_train, df_valid, dataloader, valid_loader)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model_filename", default="model.pth", type=str) # should have .pth suffix
    parser.add_argument("--device", default="cuda", type=str)
    parser.add_argument("--filename", type=str, default='classifier/toy_training_data.csv') # annotated pages from sample
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--n_epochs", type=int, default=10)
    parser.add_argument("--batch_size", type=int, default=16)
    parser.add_argument("--num_workers", type=int, default=4)
    parser.add_argument("--learning_rate", type=float, default=0.00002)
    parser.add_argument("--train_ratio", type=float, default=0.6)
    parser.add_argument("--valid_ratio", type=float, default=0.2) # test set is what remains after train and valid splits
    args = parser.parse_args()
    main(args)
