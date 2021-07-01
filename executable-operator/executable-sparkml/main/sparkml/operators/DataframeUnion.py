import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/11/26 15:05
@Author     : Jimmy
@Description: Spark Union
"""

logger = Logger('OperatorLogger').logger


class DataframeUnion(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeUnion", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df_1 = self.getInputData("data_1")
            df_2 = self.getInputData("data_2")

            self.setOutputData("result", df_1.union(df_2))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
