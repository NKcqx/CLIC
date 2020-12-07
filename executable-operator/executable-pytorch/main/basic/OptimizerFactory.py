import torch.optim as optim
"""
@ProjectName: CLIC
@Time       : 2020/12/4 下午7:40
@Author     : zjchen
@Description: 
"""


class OptimizerFactory:
    def __init__(self):
        self.optimizerMap = {
            "Adadelta": optim.Adadelta,
        }

    def createOptimizer(self, name):
        if name not in self.optimizerMap.keys():
            raise ValueError("optimizer不存在或未被初始化！")
        optimizer = self.optimizerMap[name]
        return optimizer

