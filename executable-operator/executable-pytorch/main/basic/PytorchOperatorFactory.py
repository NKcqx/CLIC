import traceback
from model import OperatorBase
from operators.PdConcat import PdConcat
from operators.PdCsvSource import PdCsvSource
from operators.PdDummies import PdDummies
from operators.PdFillNa import PdFillNa
from operators.PdIloc import PdIloc
from operators.PdStandardization import PdStandardization
from operators.TensorConverter import TensorConverter
from operators.TorchPCA import TorchPCA
from operators.TorchNet import TorchNet
from operators.PdGetSeries import PdGetSeries
from operators.Word2Vec import Word2Vec
from model.OperatorFactory import OperatorFactory

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
            "PdCsvSource": PdCsvSource,
            "PdDummies": PdDummies,
            "PdFillNa": PdFillNa,
            "PdIloc": PdIloc,
            "PdStandardization": PdStandardization,
            "TensorConverter": TensorConverter,
            "TorchPCA": TorchPCA,
            "TorchNet": TorchNet,
            "PdGetSeries": PdGetSeries,
            "Word2Vec": Word2Vec
        }

    def createOperator(self, name, id, inputKeys, outputKeys, params) -> OperatorBase:
        if name not in self.operatorMap.keys():
            raise ValueError("操作符不存在或未被初始化！")
        optCls = self.operatorMap[name]
        return optCls(id, inputKeys, outputKeys, params)
