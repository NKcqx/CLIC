import torch.nn as nn
import torch
import torch.nn.functional as F
import collections
import torchtext.vocab as Vocab


"""
Time       : 2021/8/6 2:30 下午
Author     : zjchen
Description:
"""


class Net(nn.Module):
    def __init__(self, vocab_size, embed_size, output_dim):
        super().__init__()
        self.embedding = nn.Embedding(len(vocab_size), embed_size)
        self.fc1 = nn.Linear(embed_size, output_dim)  # dense layer

    def forward(self, text):
        # text shape = (batch_size, num_sequences)
        embedded = self.embedding(text.permute(1, 0))  # embedded = [batch size, sent_len, emb dim]
        embedded = embedded.permute(1, 0, 2)  # [batch size, sent len, emb dim]
        x = F.avg_pool2d(embedded, (embedded.shape[1], 1)).squeeze(1)  # [batch size, embedding_dim]
        x = self.fc1(x)
        return x


def netProcessUdf(net, params):
    words_dict = params["words_dict"]
    result = net(46152, 100, 1000, 500, 2, 500)
    result.embedding.weight.data.copy_(load_pretrained_embedding(vocab.itos, words_dict, 100))
    result.embedding.weight.requires_grad = False  # 直接加载预训练好的, 所以不需要更新它
    return result


def load_pretrained_embedding(words, pretrained_vocab, embedding_size):
    """从预训练好的vocab中提取出words对应的词向量"""
    embed = torch.zeros(len(words), embedding_size)  # 初始化为0
    oov_count = 0  # out of vocabulary
    for i, word in enumerate(words):
        try:
            # idx = pretrained_vocab.stoi[word]
            embed[i, :] = torch.tensor(pretrained_vocab[word])
        except KeyError:
            oov_count += 1
    if oov_count > 0:
        print("There are %d oov words." % oov_count)
    return embed