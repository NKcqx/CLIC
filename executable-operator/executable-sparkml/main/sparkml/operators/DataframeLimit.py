import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/11/30 10:56
@Author     : Jimmy
@Description: 取Dataframe的前number行
"""

logger = Logger('OperatorLogger').logger


class DataframeLimit(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeLimit", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            number = int(self.params["number"])

            self.setOutputData("result", df.limit(number))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
