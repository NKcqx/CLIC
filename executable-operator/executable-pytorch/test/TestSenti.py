import os
from tqdm import tqdm
import random
import collections
import torchtext.vocab as Vocab
import torch.nn as nn
import torch
import torch.nn.functional as F

"""
Time       : 2021/7/22 1:28 下午
Author     : zjchen
Description: 仅作为测试用udf
"""


def sourceUdf(dataPath):
    data = []
    for label in ['pos', 'neg']:
        folder_name = os.path.join(dataPath, label)
        for file in tqdm(os.listdir(folder_name)):
            with open(os.path.join(folder_name, file), 'rb') as f:
                review = f.read().decode('utf-8').replace('\n', '').lower()
                data.append([review, 1 if label == 'pos' else 0])
    random.shuffle(data)
    return data


def dataProcessUdf(train_data):
    def get_tokenized_imdb(data):
        """
        data: list of [string, label]
        """
        def tokenizer(text):
            return [tok.lower() for tok in text.split(' ')]
        return [tokenizer(review) for review, _ in data]

    def get_vocab_imdb(data):  # 本函数已保存在d2lzh_pytorch包中方便以后使用
        tokenized_data = get_tokenized_imdb(data)
        counter = collections.Counter([tk for st in tokenized_data for tk in st])
        return Vocab.Vocab(counter, min_freq=5)

    vocab = get_vocab_imdb(train_data)
    print('# words in vocab:', len(vocab))

    def preprocess_imdb(data, vocab):  # 本函数已保存在d2lzh_torch包中方便以后使用
        max_l = 500  # 将每条评论通过截断或者补0，使得长度变成500

        def pad(x):
            return x[:max_l] if len(x) > max_l else x + [0] * (max_l - len(x))

        tokenized_data = get_tokenized_imdb(data)
        features = torch.tensor([pad([vocab.stoi[word] for word in words]) for words in tokenized_data])
        labels = torch.tensor([score for _, score in data])
        return features, labels

    return preprocess_imdb(train_data, vocab)


class MLP(nn.Module):
    def __init__(self, vocab_size, embed_size, hidden_size2, output_dim, max_document_length):
        super().__init__()
        # embedding and convolution layers
        self.embedding = nn.Embedding(vocab_size, embed_size)
        self.relu = nn.ReLU()
        self.fc1 = nn.Linear(embed_size*max_document_length, hidden_size2)  # dense layer
        self.fc2 = nn.Linear(hidden_size2, output_dim)  # dense layer

    def forward(self, text):
        # text shape = (batch_size, num_sequences)
        embedded = self.embedding(text)
        # embedded = [batch size, sent_len, emb dim]
        x = embedded.view(embedded.shape[0], -1)  # x = Flatten()(x)
        x = self.relu(self.fc1(x))
        preds = self.fc2(x)
        return preds


vocab_size, embed_size, hidden_size2, output_dim, max_document_length = 46152, 100, 100, 2, 500
net = MLP(vocab_size, embed_size, hidden_size2, output_dim, max_document_length)
loss = torch.nn.CrossEntropyLoss

optimizer = torch.optim.Adam(filter(lambda p: p.requires_grad, net.parameters()), lr=0.5)

