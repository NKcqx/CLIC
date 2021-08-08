import os
from tqdm import tqdm
import random
import collections
import torchtext.vocab as Vocab
import torch.nn as nn
import torch
import torch.nn.functional as F
import pandas as pd


"""
Time       : 2021/8/6 1:19 下午
Author     : zjchen
Description:
"""


# def sourceUdf(dataPath):
#     data = []
#     for label in ['pos', 'neg']:
#         folder_name = os.path.join(dataPath, label)
#         for file in tqdm(os.listdir(folder_name)):
#             with open(os.path.join(folder_name, file), 'rb') as f:
#                 review = f.read().decode('utf-8').replace('\n', '').lower()
#                 data.append([review, 1 if label == 'pos' else 0])
#     random.shuffle(data)
#     return data


def sourceUdf(dataPath):
    data = []
    raw_data = pd.read_csv(dataPath)
    for _, row in tqdm(raw_data.iterrows()):
        data.append([row["review"].lower(), 1 if row["tag"] == "__label__2" else 0])
    return data



class Net(nn.Module):
    def __init__(self, vocab_size, embed_size, hidden_size1, output_dim):
        super().__init__()
        self.embedding = nn.Embedding(len(vocab_size), embed_size)
        self.fc1 = nn.Linear(embed_size, hidden_size1)  # dense layer
        self.fc2 = nn.Linear(hidden_size1, output_dim)

    def forward(self, text):
        # text shape = (batch_size, num_sequences)
        embedded = self.embedding(text.permute(1, 0))  # embedded = [batch size, sent_len, emb dim]
        embedded = embedded.permute(1, 0, 2)  # [batch size, sent len, emb dim]
        x = F.avg_pool2d(embedded, (embedded.shape[1], 1)).squeeze(1)  # [batch size, embedding_dim]
        x = self.fc1(x)
        x = self.fc2(x)
        return x


loss = torch.nn.CrossEntropyLoss

optimizer = torch.optim.Adam

if __name__ == "__main__":
    sourceUdf("/Users/zjchen/Downloads/amazon_reviews/test.ft.csv")

