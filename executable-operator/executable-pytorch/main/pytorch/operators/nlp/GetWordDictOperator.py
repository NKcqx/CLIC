import sys
import importlib
from executable.basic.model.OperatorBase import OperatorBase
import pandas as pd
from pytorch.basic.utils import getModuleByUdf
import torch.utils.data as Data
from torch import tensor


"""
Time       : 2021/8/6 4:48 下午
Author     : zjchen
Description:
"""


class GetWordDictOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("GetWordDictOperator", ID, inputKeys, outputKeys, Params)

    def execute(self):
        w2v_dataframe = pd.read_csv(self.getInputData("data"), converters={0: str, 1: eval}, error_bad_lines=False)  # , header=None可选参数
        w2v_dict = {}
        for index, row in w2v_dataframe.iterrows():
            w2v_dict[row[0]] = row[1]
        self.setOutputData("result", w2v_dict)

