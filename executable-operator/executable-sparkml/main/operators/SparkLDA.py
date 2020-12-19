import traceback

from model.OperatorBase import OperatorBase
from pyspark.ml.clustering import LDA

"""
@ProjectName: CLIC
@Time       : 2020/12/1 14:47
@Author     : jimmy
@Description: Spark LDA
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

            lda = LDA(featuresCol=col, optimizer=optimizer, k=k, topicDistributionCol=output_label)

            result = lda.fit(df).transform(df)

            self.setOutputData("result", result)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
