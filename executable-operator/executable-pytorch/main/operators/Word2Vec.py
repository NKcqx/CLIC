import traceback
import collections
import math
import random
import sys
import time
import torch
from torch import nn
import torch.utils.data as Data
from model.OperatorBase import OperatorBase
from basic.LossFunctionFactory import LossFunctionFactory
from basic.OptimizerFactory import OptimizerFactory
"""
@ProjectName: CLIC
@Time       : 2020/12/13 上午11:48
@Author     : zjchen
@Description: 
"""


class MyDataset(torch.utils.data.Dataset):
    def __init__(self, centers, contexts, negatives):
        assert len(centers) == len(contexts) == len(negatives)
        self.centers = centers
        self.contexts = contexts
        self.negatives = negatives

    def __getitem__(self, index):
        return self.centers[index], self.contexts[index], self.negatives[index]

    def __len__(self):
        return len(self.centers)


def discard(idx, counter, idx_to_token, num_tokens):
    return random.uniform(0, 1) < 1 - math.sqrt(1e-4 / counter[idx_to_token[idx]] * num_tokens)


def compare_counts(token, token_to_idx, dataset, subsampled_dataset):
    return '# %s: before=%d, after=%d' % (token, sum([st.count(token_to_idx[token]) for st in dataset]),
                                          sum([st.count(token_to_idx[token]) for st in subsampled_dataset]))


def get_centers_and_contexts(dataset, max_window_size):
    centers, contexts = [], []
    for st in dataset:
        if len(st) < 2:  # 每个句子至少要有2个词才可能组成一对“中心词-背景词”
            continue
        centers += st
        for center_i in range(len(st)):
            window_size = random.randint(1, max_window_size)
            indices = list(range(max(0, center_i - window_size),
                                 min(len(st), center_i + 1 + window_size)))
            indices.remove(center_i)  # 将中心词排除在背景词之外
            contexts.append([st[idx] for idx in indices])
    return centers, contexts


def get_negatives(all_contexts, sampling_weights, K):
    all_negatives, neg_candidates, i = [], [], 0
    population = list(range(len(sampling_weights)))
    for contexts in all_contexts:

        negatives = []
        while len(negatives) < len(contexts) * K:
            if i == len(neg_candidates):
                # 根据每个词的权重（sampling_weights）随机生成k个词的索引作为噪声词。
                # 为了高效计算，可以将k设得稍大一点
                i, neg_candidates = 0, random.choices(
                    population, sampling_weights, k=int(1e5))
            neg, i = neg_candidates[i], i + 1
            # 噪声词不能是背景词
            if neg not in set(contexts):
                negatives.append(neg)
        all_negatives.append(negatives)
    return all_negatives


def batchify(data):
    """用作DataLoader的参数collate_fn: 输入是个长为batchsize的list, list中的每个元素都是__getitem__得到的结果"""
    max_len = max(len(c) + len(n) for _, c, n in data)
    centers, contexts_negatives, masks, labels = [], [], [], []
    for center, context, negative in data:
        cur_len = len(context) + len(negative)
        centers += [center]
        contexts_negatives += [context + negative + [0] * (max_len - cur_len)]
        masks += [[1] * cur_len + [0] * (max_len - cur_len)]
        labels += [[1] * len(context) + [0] * (max_len - len(context))]
    return (torch.tensor(centers).view(-1, 1), torch.tensor(contexts_negatives),
            torch.tensor(masks), torch.tensor(labels))


def skip_gram(center, contexts_and_negatives, embed_v, embed_u):
    v = embed_v(center)
    u = embed_u(contexts_and_negatives)
    pred = torch.bmm(v, u.permute(0, 2, 1))
    return pred


class Word2Vec(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("Word2Vec", ID, inputKeys, outputKeys, Params)

    def train(self, net, num_epochs, data_iter, loss, optimizer):
        loss = loss()
        device = self.params["device"]
        print("train on", device)
        net = net.to(device)
        for epoch in range(num_epochs):
            start, l_sum, n = time.time(), 0.0, 0
            for batch in data_iter:
                center, context_negative, mask, label = [d.to(device) for d in batch]

                pred = skip_gram(center, context_negative, net[0], net[1])

                # 使用掩码变量mask来避免填充项对损失函数计算的影响
                l = (loss(pred.view(label.shape), label, mask) *
                     mask.shape[1] / mask.float().sum(dim=1)).mean()  # 一个batch的平均loss
                optimizer.zero_grad()
                l.backward()
                optimizer.step()
                l_sum += l.cpu().item()
                n += 1
            print('epoch %d, loss %.2f, time %.2fs'
                  % (epoch + 1, l_sum / n, time.time() - start))
        self.setOutputData("result", net)

    def execute(self):
        try:
            lossFunctionFactory = LossFunctionFactory()
            optimizerFactory = OptimizerFactory()
            loss_function = lossFunctionFactory.createLossFunction(self.params["loss"])
            optimizer = optimizerFactory.createOptimizer(self.params["optimizer"])
            batch_size = self.params["batch_size"]
            num_workers = self.params["num_workers"]
            min_count = self.params["min_count"]
            max_window_size = self.params["max_window_size"]
            lr = self.params["lr"]

            raw_dataset = [i.split() for i in self.getInputData("data")]
            # 筛选出现次数大于5次的词
            counter = collections.Counter([tk for st in raw_dataset for tk in st])
            counter = dict(filter(lambda x: x[1] >= min_count, counter.items()))

            # 建立词语索引
            idx_to_token = [tk for tk, _ in counter.items()]
            token_to_idx = {tk: idx for idx, tk in enumerate(idx_to_token)}
            dataset = [[token_to_idx[tk] for tk in st if tk in token_to_idx] for st in raw_dataset]
            num_tokens = sum([len(st) for st in dataset])

            # 二次采样，丢弃高频词
            subsampled_dataset = [[tk for tk in st if not discard(tk, counter, idx_to_token, num_tokens)] for st in dataset]

            # 提取中心词和背景词
            all_centers, all_contexts = get_centers_and_contexts(subsampled_dataset, max_window_size)

            # 负采样
            sampling_weights = [counter[w] ** 0.75 for w in idx_to_token]
            all_negatives = get_negatives(all_contexts, sampling_weights, 5)

            # batchify函数指定DataLoader实例中⼩批量的读取⽅式。
            dataset = MyDataset(all_centers,
                                all_contexts,
                                all_negatives)

            data_iter = Data.DataLoader(dataset, batch_size, shuffle=True,
                                        collate_fn=batchify,
                                        num_workers=num_workers)

            # 初始化模型参数
            embed_size = self.params["embed_size"]
            net = nn.Sequential(
                nn.Embedding(num_embeddings=len(idx_to_token), embedding_dim=embed_size),
                nn.Embedding(num_embeddings=len(idx_to_token), embedding_dim=embed_size)
            )
            optimizer = optimizer(net.parameters(), lr=lr)
            print("开始训练")
            self.train(net, num_epochs=self.params["num_epochs"], data_iter=data_iter, loss=loss_function, optimizer=optimizer)

        except Exception as e:
            print(e)
            print("=" * 20)
            print(traceback.format_exc())
