import traceback

from model.OperatorBase import OperatorBase
from pyspark.ml.clustering import LDA
import pyspark.ml.feature as ft
from pyspark.ml import Pipeline

"""
@ProjectName: CLIC
@Time       : 2020/12/1 14:47
@Author     : jimmy
@Description: 
"""


class SparkLDA(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkLDA", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            col = self.params["col"]
            optimizer = self.params["optimizer"]
            k = self.params["k"]
            output_label = self.params["output_label"]

            # 去停用词
            stopwords = ft.StopWordsRemover(inputCol=col, outputCol=col + '-stop')

            # 统计词频
            stringIndex = ft.CountVectorizer(inputCol=col + '-stop', outputCol=col + '-indexed')

            # LDA
            lda = LDA(featuresCol=col + '-indexed', optimizer=optimizer, k=k, topicDistributionCol=output_label)

            pipline = Pipeline(stages=[stopwords, stringIndex, lda])
            result = pipline.fit(df) \
                .transform(df) \
                .drop(col+'-stop') \
                .drop(col+'-indexed')

            self.setOutputData("result", result)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
