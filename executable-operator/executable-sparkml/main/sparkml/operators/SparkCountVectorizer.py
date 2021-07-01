import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.feature import CountVectorizer

"""
@ProjectName: CLIC
@Time       : 2020/12/7 13:59
@Author     : Jimmy
@Description: Spark统计词频
"""

logger = Logger('OperatorLogger').logger


class SparkCountVectorizer(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkCountVectorizer", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_col = self.params["inputCol"]
            output_col = self.params["outputCol"]

            vectorizer = CountVectorizer(inputCol=input_col, outputCol=output_col)

            self.setOutputData("result", vectorizer.fit(df).transform(df))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
