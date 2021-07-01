import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.clustering import LDA

"""
@ProjectName: CLIC
@Time       : 2020/12/1 14:47
@Author     : Jimmy
@Description: Spark ML LDA
"""

logger = Logger('OperatorLogger').logger


class SparkLDA(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkLDA", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            col = self.params["col"]
            optimizer = self.params["optimizer"]
            k = int(self.params["k"])
            output_label = self.params["outputLabel"]

            lda = LDA(featuresCol=col, optimizer=optimizer, k=k, topicDistributionCol=output_label)

            result = lda.fit(df).transform(df)

            self.setOutputData("result", result)

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
