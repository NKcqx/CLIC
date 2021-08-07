import traceback
import sys
import importlib
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
import pandas as pd
from pytorch.basic.utils import getModuleByUdf
import torch.utils.data as Data
from torch import tensor


"""
Time       : 2021/8/6 4:48 下午
Author     : zjchen
Description:
"""

logger = Logger('OperatorLogger').logger


class GetWordDictOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("GetWordDictOperator", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            w2v_dataframe = pd.read_csv(self.getInputData("data"), converters={"word": str, "vector": eval})
            w2v_dict = {}
            for index, row in w2v_dataframe.iterrows():
                w2v_dict[row["word"]] = row["vector"]
            self.setOutputData("result", w2v_dict)

        except Exception as e:
            logger.error(traceback.format_exc())
