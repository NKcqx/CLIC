import traceback
import sys
import importlib
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pytorch.basic.utils import getModuleByUdf
import torch
import torch.utils.data as Data
from torch import tensor

"""
Time       : 2021/8/5 11:51 上午
Author     : zjchen
Description:
"""


logger = Logger('OperatorLogger').logger


class NetProcessOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("NetProcessOperator", ID, inputKeys, outputKeys, Params)
        self.module = getModuleByUdf(self.params["udfPath"])

    def execute(self):
        try:
            vocab = self.getInputData("vocab")
            w2v_dict = self.getInputData("w2v_dict")
            net = self.module.Net(vocab, 100, 100, 2)
            net.embedding.weight.data.copy_(load_pretrained_embedding(vocab.itos, w2v_dict, 100))
            net.embedding.weight.requires_grad = False  # 直接加载预训练好的, 所以不需要更新它
            self.setOutputData("result", net)

        except Exception as e:
            logger.error(traceback.format_exc())


def load_pretrained_embedding(words, pretrained_vocab, embedding_size):
    """从预训练好的vocab中提取出words对应的词向量"""
    embed = torch.zeros(len(words), embedding_size)  # 初始化为0
    oov_count = 0 # out of vocabulary
    for i, word in enumerate(words):
        try:
            # idx = pretrained_vocab.stoi[word]
            embed[i, :] = torch.tensor(pretrained_vocab[word])
        except KeyError:
            oov_count += 1
    if oov_count > 0:
        logger.info("There are %d oov words." % oov_count)
    return embed

