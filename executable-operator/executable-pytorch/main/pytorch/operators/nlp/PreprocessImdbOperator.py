
import traceback
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
import torch


"""
Time       : 2021/8/6 3:55 下午
Author     : zjchen
Description:
"""


logger = Logger('OperatorLogger').logger


class PreprocessImdbOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PreprocessImdbOperator", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            self.setOutputData("result", self.preprocess_imdb(self.getInputData("data"), self.getInputData("vocab"),
                                                              self.getInputData("tokenized_data")))
        except Exception as e:
            logger.error(traceback.format_exc())

    def preprocess_imdb(self, data, vocab, tokenized_data):  # 本函数已保存在d2lzh_torch包中方便以后使用
        max_l = self.params["max_l"]  # 将每条评论通过截断或者补0，使得长度变成500

        def pad(x):
            return x[:max_l] if len(x) > max_l else x + [0] * (max_l - len(x))

        features = torch.tensor([pad([vocab.stoi[word] for word in words]) for words in tokenized_data])
        labels = torch.tensor([score for _, score in data])
        return [features, labels]
