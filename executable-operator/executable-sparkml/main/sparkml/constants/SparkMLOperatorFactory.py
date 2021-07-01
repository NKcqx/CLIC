from executable.basic.model.OperatorFactory import OperatorFactory
from sparkml.operators.DataframeColumns import DataframeColumns
from sparkml.operators.DataframeDrop import DataframeDrop
from sparkml.operators.DataframeFillNa import DataframeFillNa
from sparkml.operators.DataframeFilter import DataframeFilter
from sparkml.operators.DataframeLimit import DataframeLimit
from sparkml.operators.DataframeUnion import DataframeUnion
from sparkml.operators.DataframeWithColumn import DataframeWithColumn
from sparkml.operators.FileSink import FileSink
from sparkml.operators.SparkLinearRegression import SparkLinearRegression
from sparkml.operators.SparkOneHotEncode import SparkOneHotEncode
from sparkml.operators.SparkCountVectorizer import SparkCountVectorizer
from sparkml.operators.SparkLDA import SparkLDA
from sparkml.operators.SparkPCA import SparkPCA
from sparkml.operators.SparkReadCSV import SparkReadCSV
from sparkml.operators.SparkRegexTokenizer import SparkRegexTokenizer
from sparkml.operators.SparkStandardScaler import SparkStandardScaler
from sparkml.operators.SparkStopWordsRemover import SparkStopWordsRemover
from sparkml.operators.SparkTransform import SparkTransform
from sparkml.operators.SparkVectorAssembler import SparkVectorAssembler

"""
@ProjectName: CLIC
@Time       : 2020/11/30 10:18
@Author     : Jimmy
@Description: 
"""


class SparkMLOperatorFactory(OperatorFactory):
    def __init__(self):
        self.operatorMap = {
            "DataframeColumns": DataframeColumns,
            "DataframeDrop": DataframeDrop,
            "DataframeFillNa": DataframeFillNa,
            "DataframeFilter": DataframeFilter,
            "DataframeLimit": DataframeLimit,
            "DataframeUnion": DataframeUnion,
            "DataframeWithColumn": DataframeWithColumn,
            "FileSink": FileSink,
            "SparkCountVectorizer": SparkCountVectorizer,
            "SparkLinearRegression": SparkLinearRegression,
            "SparkOneHotEncode": SparkOneHotEncode,
            "SparkLDA": SparkLDA,
            "SparkPCA": SparkPCA,
            "SparkReadCSV": SparkReadCSV,
            "SparkRegexTokenizer": SparkRegexTokenizer,
            "SparkStandardScaler": SparkStandardScaler,
            "SparkStopWordsRemover": SparkStopWordsRemover,
            "SparkTransform": SparkTransform,
            "SparkVectorAssembler": SparkVectorAssembler
        }

    def createOperator(self, name, id, inputKeys, outputKeys, params):
        if name not in self.operatorMap.keys():
            raise ValueError("操作符不存在或未被初始化！")
        optCls = self.operatorMap[name]
        return optCls(id, inputKeys, outputKeys, params)
