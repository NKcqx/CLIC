from pytorch.basic.TrainUtils import train
from pytorch.basic.utils import getModuleByUdf
from executable.basic.model.OperatorBase import OperatorBase
from pytorch.basic.LossFunctionFactory import LossFunctionFactory
from pytorch.basic.OptimizerFactory import OptimizerFactory
from pytorch.basic.NetFactory import NetFactory


"""
Time       : 2021/7/15 3:37 下午
Author     : zjchen
Description: 用于训练神经网络的operator，使用了一个通用的训练模版, 
             这里的问题在于，要求用户提前提供pytorch版本的net, loss, optimizer，
             但是从逻辑上来说，用户是不能提前知道这个算子将在什么地方运行
"""


class TrainOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("TrainOperator", ID, inputKeys, outputKeys, Params)
        self.kwargs = dict()
        self.netFactory = NetFactory()
        self.lossFactory = LossFunctionFactory()
        self.optimizerFactory = OptimizerFactory()

    def fillKwargs(self, Net):
        """
        找出udf中的net, loss, optimizer， 这一步要求用户在udf中添加名称一样的类，只会检查固定的几个类
        """
        # try:
        module = getModuleByUdf(self.params["udfPath"])
        tempDict = {
            "num_epochs": eval(self.params["num_epochs"]),
            # 名称暂时必须得固定一致
            # "net": module.net if "net" in dir(module) else self.netFactory.createNet(self.params["net"]),
            "loss": module.loss() if "loss" in dir(module) else self.lossFactory.createLossFunction(self.params["loss"]),
            "tol_threshold": eval(self.params["tol_threshold"]),
            "optimizer": module.optimizer(filter(lambda p: p.requires_grad, Net.parameters()), lr=0.0001) if "optimizer" in dir(module) else self.optimizerFactory.createOptimizer(self.params["optimizer"])
        }
        self.kwargs.update(tempDict)

    def execute(self):
        self.kwargs["net"] = self.getInputData("net")
        self.fillKwargs(self.kwargs["net"])

        # kwargs需要train_iter, net, loss, optimizer, device, num_epochs这些参数
        self.setOutputData("result", train(self.getInputData("data"), **self.kwargs))


