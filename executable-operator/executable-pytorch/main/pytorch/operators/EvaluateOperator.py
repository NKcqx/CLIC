import traceback
import sys
import importlib
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pytorch.basic.TrainUtils import evaluate_accuracy


"""
Time       : 2021/7/21 4:56 下午
Author     : zjchen
Description:
"""

logger = Logger('OperatorLogger').logger


class EvaluateOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("EvaluateOperator", ID, inputKeys, outputKeys, Params)
        self.module = None

    def execute(self):
        try:
            self.setOutputData("result", evaluate_accuracy(self.getInputData("data_iter"), self.getInputData("net"),
                                                           self.params["device"]))
        except Exception as e:
            logger.error(traceback.format_exc())
