from pytorch.operators.PdConcat import PdConcat
from pytorch.operators.PdCsvSource import PdCsvSource
from pytorch.operators.PdDummies import PdDummies
from pytorch.operators.PdFillNa import PdFillNa
from pytorch.operators.PdIloc import PdIloc
from pytorch.operators.PdStandardization import PdStandardization
from pytorch.operators.TensorConverter import TensorConverter
from pytorch.operators.TorchPCA import TorchPCA
from pytorch.operators.TorchNet import TorchNet
from pytorch.operators.PdGetSeries import PdGetSeries
from pytorch.operators.Word2Vec import Word2Vec
from pytorch.operators.FileSink import FileSink
from executable.basic.model.OperatorFactory import OperatorFactory
from executable.basic.model.OperatorBase import OperatorBase


"""
@ProjectName: CLIC
@Time       : 2020/11/25 下午4:22
@Author     : zjchen
@Description: 
"""


class PytorchOperatorFactory(OperatorFactory):
    def __init__(self):
        self.operatorMap = {
            "PdConcat": PdConcat,
            "SourceOperator": PdCsvSource,
            "PdDummies": PdDummies,
            "PdFillNa": PdFillNa,
            "PdIloc": PdIloc,
            "PdStandardization": PdStandardization,
            "TensorConverter": TensorConverter,
            "TorchPCA": TorchPCA,
            "TorchNet": TorchNet,
            "PdGetSeries": PdGetSeries,
            "Word2Vec": Word2Vec,
            "SinkOperator": FileSink,
        }

    def createOperator(self, name, id, inputKeys, outputKeys, params) -> OperatorBase:
        if name not in self.operatorMap.keys():
            raise ValueError(name + "操作符不存在或未被初始化！")
        optCls = self.operatorMap[name]
        return optCls(id, inputKeys, outputKeys, params)
