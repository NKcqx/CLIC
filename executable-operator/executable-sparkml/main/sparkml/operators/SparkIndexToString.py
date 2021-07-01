import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.feature import IndexToString

"""
@ProjectName: CLIC
@Time       : 2021/4/12 17:47
@Author     : Jimmy
@Description: 
"""

logger = Logger('OperatorLogger').logger


class SparkIndexToString(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkIndexToString", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_col = self.params["inputCol"]
            output_col = self.params["outputCol"]

            indexer = IndexToString(inputCol=input_col, outputCol=output_col)

            self.setOutputData("result", indexer.transform(df))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
