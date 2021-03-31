import torch
import torch.nn.functional
from operators.Loss.SigmoidBinaryCrossEntropyLoss import SigmoidBinaryCrossEntropyLoss
"""
@ProjectName: CLIC
@Time       : 2020/12/3 下午9:42
@Author     : zjchen
@Description: 
"""


class LossFunctionFactory:
    def __init__(self):
        self.lossFunctionMap = {
            "MSELoss": torch.nn.MSELoss,
            "CrossEntropyLoss": torch.nn.CrossEntropyLoss,
            "nll_loss": torch.nn.functional.nll_loss,
            "SigmoidBinaryCrossEntropyLoss": SigmoidBinaryCrossEntropyLoss,
        }

    def createLossFunction(self, name):
        if name not in self.lossFunctionMap.keys():
            raise ValueError("LossFunction不存在或未被初始化！")
        loss = self.lossFunctionMap[name]
        return loss



