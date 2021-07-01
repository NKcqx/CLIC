import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/11/30 11:35
@Author     : Jimmy
@Description: 获取dataframe的全部列名
"""

logger = Logger('OperatorLogger').logger


class DataframeColumns(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeColumns", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")

            self.setOutputData("result", df.columns)

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
