from pytorch.operators.SourceOperator import SourceOperator
from pytorch.operators.SinkOperator import SinkOperator
from pytorch.operators.TrainOperator import TrainOperator
from pytorch.operators.EvaluateOperator import EvaluateOperator
from pytorch.operators.DataLoadOperator import DataLoadOperator
from pytorch.operators.nlp.TokenizedOperator import TokenizedOperator
from pytorch.operators.nlp.GetVocabOperator import GetVocabOperator
from pytorch.operators.nlp.NetPreprocessOperator import NetProcessOperator
from pytorch.operators.nlp.PreprocessImdbOperator import PreprocessImdbOperator
from pytorch.operators.nlp.GetWordDictOperator import GetWordDictOperator

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
            "SinkOperator": SinkOperator,
            "SourceOperator": SourceOperator,
            "TrainOperator": TrainOperator,
            "EvaluateOperator": EvaluateOperator,
            "DataLoadOperator": DataLoadOperator,
            "TokenizedOperator": TokenizedOperator,
            "GetVocabOperator": GetVocabOperator,
            "NetProcessOperator": NetProcessOperator,
            "PreprocessImdbOperator": PreprocessImdbOperator,
            "GetWordDictOperator": GetWordDictOperator,
        }

    def createOperator(self, name, id, inputKeys, outputKeys, params) -> OperatorBase:
        if name not in self.operatorMap.keys():
            raise ValueError(name + "操作符不存在或未被初始化！")
        optCls = self.operatorMap[name]
        return optCls(id, inputKeys, outputKeys, params)
