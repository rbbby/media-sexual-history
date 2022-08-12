import numpy as np
import torch
import torch.nn as nn
from tqdm import tqdm
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, roc_curve
import pandas as  pd
from plotnine import ggplot, aes, geom_line, geom_abline, ggtitle, scale_x_discrete, scale_y_discrete


def predict(df, testloader, model, device):
    """
    Args:
        mix (bool): True if both global and local image features were used.
        False otherwise.
    """
    loss_fn = nn.BCEWithLogitsLoss()
    probs_list = []
    running_loss = 0
    model.eval()
    with torch.no_grad():
        for i, batch in enumerate(tqdm(testloader)):
            labels = batch[1]
            token_ids = batch[0]["input_ids"].squeeze(dim=1)
            type_ids = batch[0]["token_type_ids"].squeeze(dim=1)
            mask = batch[0]["attention_mask"].squeeze(dim=1)
            numeric_features = batch[2]
            categorical_features = batch[3]
            output = model(token_ids=token_ids.to(device),
                           type_ids=type_ids.to(device),
                           mask=mask.to(device),
                           numeric_features=numeric_features.to(device),
                           categorical_features=categorical_features.to(device))

            labels = labels.unsqueeze(1).type_as(output)
            loss = loss_fn(output, labels)
            running_loss += loss.item()
            probs_list += torch.sigmoid(output).tolist()


        df["probs"] = sum(probs_list, [])
        df.sort_values(by="probs", ascending=False)

        df["pred"] = 1
        df.loc[df["probs"] < 0.5, "pred"] = 0

    return df, running_loss


def get_metrics(df):
    metrics = {
        "accuracy": accuracy_score(y_true=df["tag"], y_pred=df["pred"]),
        "f1": f1_score(y_true=df["tag"], y_pred=df["pred"]),
        "precision": precision_score(y_true=df["tag"], y_pred=df["pred"]),
        # Sensitivity (# correct ad preds / # true ads)
        "sensitivity": recall_score(y_true=df["tag"], y_pred=df["pred"]),
        # Specificity (# correct editorial preds / # true editorial)
        "specificity": recall_score(y_true=df["tag"], y_pred=df["pred"], pos_label=0),
        "roc_auc": roc_auc_score(y_true=df["tag"], y_score=df["probs"])
    }

    print(metrics)
    return metrics


def plot_roc_auc(df):
    fpr, tpr, threshold = roc_curve(df["tag"], df["probs"])
    df_plot = pd.DataFrame(dict(fpr=fpr, tpr=tpr))

    p = (
        ggplot(data=df_plot)
        + aes(x="fpr", y="tpr")
        + geom_line()
        + geom_abline(linetype="dashed")
        + ggtitle("ROC AUC for newspaper bodytext classification")
    )

    return p


def plot_train_eval_loss(train_loss, eval_loss):
    df_plot = pd.DataFrame({'Training loss':train_loss,\
                            'Evaluation loss':eval_loss})

    df_plot = pd.melt(df_plot.reset_index(), id_vars='index')
    df_plot = df_plot.rename(columns={'index':'Epoch', 'value':'Loss'})
    df_plot['Epoch'] += 1

    p = (
        ggplot(data=df_plot)
        + aes(x="Epoch", y='Loss', color='variable')
        + geom_line()
        + ggtitle("Train/eval loss for newspaper bodytext classification")
        + scale_x_discrete(limits=np.arange(1, len(train_loss)+1))
        #+ scale_y_discrete(breaks=np.arange(0, 200+1, 50))
        )

    return p

def _plot_metric(loss, metric='accuracy'):
    df_plot = pd.DataFrame({'Epoch':list(range(1, len(loss)+1)), metric.title():loss})

    p = (
        ggplot(data=df_plot)
        + aes(x="Epoch", y=metric.title())
        + geom_line()
        + ggtitle("Evaluation loss for newspaper bodytext classification")
    )

    return p


def plot_lr(lrs):
    df_plot = pd.DataFrame({'Iteration':list(range(1, len(lrs)+1)), 'Learning rate':lrs})

    p = (
        ggplot(data=df_plot)
        + aes(x="Iteration", y='Learning rate')
        + geom_line()
        + ggtitle("Learning rate schedule for newspaper bodytext classification")
    )
    
    return p